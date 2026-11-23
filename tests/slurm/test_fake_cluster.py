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
    sacct = (cluster.bin / 'sacct.out').read_text().splitlines()
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
    sacct = lambda: (cluster.bin / 'sacct.out').read_text().splitlines()  # noqa: E731
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
