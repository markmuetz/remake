"""The simulated cluster's own behaviour (fake_cluster.py): submission
through remake's real submit.sh, dependencies, kills, rendering."""
from pathlib import Path

from remake.remake_cmd import remake_cmd


def cli(*args):
    return remake_cmd(['remake', *args])


def jobs_by_name(cluster):
    cluster.sync()
    return {job.name: job for job in cluster.jobs.values()}


def test_submission_is_picked_up_with_dependencies(cluster):
    assert cli('run', 'pipeline.py', '-E', 'slurm') == 0
    jobs = jobs_by_name(cluster)
    assert set(jobs) == {'gen', 'proc', 'agg'}
    assert jobs['gen'].indices == [0, 1, 2, 3]
    assert jobs['proc'].deps == [('aftercorr', jobs['gen'].jobid)]
    assert jobs['agg'].deps == [('afterok', jobs['proc'].jobid)]
    assert jobs['proc'].kill_on_invalid_dep
    # Only gen's elements can start.
    assert sorted(cluster.eligible()) == [(jobs['gen'].jobid, i) for i in range(4)]


def test_drain_runs_the_real_payload(cluster, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.drain()
    assert Path('data/agg.txt').read_text() == '00,11,22,33'
    assert all(el.state == 'COMPLETED'
               for job in cluster.jobs.values() for el in job.elements.values())
    # The results reached the DB through the sidecars.
    capsys.readouterr()
    cli('run', 'pipeline.py', '-n')
    assert '0 task(s) would run' in capsys.readouterr().out


def test_kill_cancels_dependents(cluster):
    cli('run', 'pipeline.py', '-E', 'slurm')
    jobs = jobs_by_name(cluster)
    gen, proc, agg = (jobs[n].jobid for n in ('gen', 'proc', 'agg'))
    cluster.kill(gen, 2, 'NODE_FAIL')
    # aftercorr: proc_2 can never run; afterok: agg can never run.
    assert jobs['proc'].elements[2].state == 'CANCELLED'
    assert jobs['agg'].elements[0].state == 'CANCELLED'
    assert jobs['proc'].elements[1].state == 'PENDING'
    cluster.drain()
    assert jobs['proc'].elements[1].state == 'COMPLETED'


def test_squeue_and_sacct_rendering(cluster):
    cli('run', 'pipeline.py', '-E', 'slurm')
    gen = jobs_by_name(cluster)['gen'].jobid
    cluster.start(gen, 0)
    squeue = (cluster.bin / 'squeue.out').read_text().splitlines()
    assert f'{gen}_0 R None' in squeue and f'{gen}_1 PD None' in squeue
    cluster.kill(gen, 0, 'NODE_FAIL')
    sacct = cluster.sacct_text().splitlines()
    row = next(r for r in sacct if r.startswith(f'{gen}_0|'))
    assert row.split('|')[1] == 'NODE_FAIL' and row.split('|')[6] == 'host1'
    # Finished elements leave squeue at once (as on JASMIN)...
    assert f'{gen}_0 ' not in (cluster.bin / 'squeue.out').read_text()
    # ...unless MinJobAge keeps them for a few ticks.
    cluster.min_job_age = 2
    cluster.render()
    assert f'{gen}_0 NF None' in (cluster.bin / 'squeue.out').read_text()


def test_faults(cluster):
    import subprocess as sp

    cluster.fail('squeue')
    assert sp.run(['squeue'], capture_output=True).returncode == 1
    cluster.fail('squeue', on=False)
    assert sp.run(['squeue'], capture_output=True).returncode == 0
    assert len(cluster.calls('squeue')) == 2


def test_sacct_never_started_rows_follow_the_live_probe(cluster):
    # tests/slurm/fixtures/jasmin_probe_2026-11-23/README.md
    cli('run', 'pipeline.py', '-E', 'slurm')
    jobs = jobs_by_name(cluster)
    gen, proc, agg = (jobs[n].jobid for n in ('gen', 'proc', 'agg'))
    sacct = lambda: cluster.sacct_text().splitlines()  # noqa: E731
    # proc (aftercorr) waiting: one row labelled with a single index.
    assert [r.split('|')[0] for r in sacct() if r.startswith(proc)] == [f'{proc}_3']
    # agg (afterok) waiting: a bracketed range.
    assert [r.split('|')[0] for r in sacct() if r.startswith(agg)] == [f'{agg}_[0]']
    cluster.kill(gen, 1, 'NODE_FAIL')
    rows = {r.split('|')[0]: r.split('|') for r in sacct()}
    # proc_1 (aftercorr-cancelled) has no row; agg's remainder is CANCELLED.
    assert f'{proc}_1' not in rows
    assert rows[f'{agg}_[0]'][1] == 'CANCELLED' and rows[f'{agg}_[0]'][3] == 'None'
    assert rows[f'{agg}_[0]'][6] == 'None assigned'
    # A user's scancel of one pending element: no row either.
    cluster.cancel(gen, 3)
    assert not any(r.startswith(f'{gen}_3|') for r in sacct())


def test_replays_the_live_probe(cluster):
    """The probe's A-D sequence (fixtures/jasmin_probe_2026-11-23/drive.sh)
    through the shims gives the same sacct rows as JASMIN did: the same
    JobIDs, states, and started-or-not (ids, times and nodes aside)."""
    import subprocess as sp

    from remake.executors.slurm_accounting import parse_sacct

    probe = Path(__file__).parent / 'fixtures' / 'jasmin_probe_2026-11-23'

    def sbatch(*args):
        return sp.run(['sbatch', '--parsable', *args], check=True, capture_output=True,
                      text=True, env=cluster.path_env()).stdout.strip()

    A = sbatch(str(probe / 'elem.sh'))
    B = sbatch('--array=0-5', f'--dependency=aftercorr:{A}', str(probe / 'dep.sh'))
    C = sbatch('--array=0-3', f'--dependency=afterok:{A}', str(probe / 'dep.sh'))
    D = sbatch('--array=0-3', str(probe / 'dep.sh'))  # --begin=+1h: never eligible
    cluster.sync()
    for i in range(6):
        cluster.start(A, i)
    cluster.finish(A, 0)
    cluster.kill(A, 1, 'FAILED')
    cluster.kill(A, 4, 'OUT_OF_MEMORY')
    # t15: B's unsplit remainder is one row held by B_5.
    t15 = parse_sacct(cluster.sacct_text())
    assert [(idx, el.state) for idx, el in t15[B].unsplit] == [(None, 'PENDING')]
    assert cluster.sacct_text().count(f'{B}_5|') == 1
    cluster.start(B, 0)
    cluster.finish(B, 0)
    cluster.cancel(A, 3)  # running
    cluster.cancel(D, 1)  # pending, alone
    cluster.tick(62)
    cluster.kill(A, 2, 'TIMEOUT')
    cluster.finish(A, 5)
    cluster.start(B, 5)
    cluster.finish(B, 5)
    cluster.cancel(D)  # the whole pending rest

    def shape(text, ids):
        names = dict(zip(ids, 'ABCD'))
        rows = set()
        for line in text.splitlines():
            if not line.strip() or line.startswith('rc='):
                continue
            cols = line.split('|')
            if len(cols) == 8:  # the probe's JobName column
                del cols[1]
            base, _, idx = cols[0].partition('_')
            if base in names:
                rows.add((names[base], idx, cols[1].split()[0], cols[3] != 'None'))
        return rows

    i = dict(kv.split('=') for kv in (probe / 'ids.txt').read_text().split())
    real = shape((probe / 'sacct_epoch_final.psv').read_text(), [i[k] for k in 'ABCD'])
    assert shape(cluster.sacct_text(), [A, B, C, D]) == real
