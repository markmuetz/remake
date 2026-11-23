"""What happens when SLURM kills a task (design_docs/designs/slurm_reconcile/):
submitted tasks are PENDING until their results arrive, so a killed task
is rerun; `slurm-status`, `why` and `task-info` say what SLURM did."""
import json
from pathlib import Path

from remake.loader import load_remake
from remake.metadata.metadata_manager import TASK_STATUS_PENDING, TASK_STATUS_SUCCESS
from remake.remake_cmd import remake_cmd


def cli(*args):
    return remake_cmd(['remake', *args])


def jobs(cluster):
    cluster.sync()
    return {job.name: job for job in cluster.jobs.values()}


def status(rule, **kwargs):
    rmk = load_remake('pipeline.py')
    task = next(t for t in rmk.tasks() if t.rule.name == rule and t.kwargs == kwargs)
    rmk.metadata.ingest_sidecars(rmk.rules)
    return rmk.metadata.get_tasks_status([task])[task.key].status


def planned(capsys):
    capsys.readouterr()
    cli('run', 'pipeline.py', '-n')
    out = capsys.readouterr().out
    return int(out.split(' task(s) would run')[0].split()[-1])


def test_all_succeed(cluster, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.drain()
    assert planned(capsys) == 0


def test_killed_forced_rerun_is_rerun(cluster, capsys):
    # The hole: a forced rerun killed by SLURM left the older success, so
    # the task looked up to date.
    cli('run', 'pipeline.py')  # everything succeeds locally
    cli('run', 'pipeline.py', '-E', 'slurm', '-f')
    assert status('gen', n=1) == TASK_STATUS_PENDING
    gen = jobs(cluster)['gen'].jobid
    cluster.kill(gen, 1, 'NODE_FAIL')
    cluster.drain()
    assert status('gen', n=1) == TASK_STATUS_PENDING
    assert status('gen', n=0) == TASK_STATUS_SUCCESS
    # gen 1, its aftercorr dependent proc 1, and agg (afterok) rerun.
    assert planned(capsys) == 3


def test_killed_first_run_partial_output_not_adopted(cluster, capsys):
    Path('pipeline.py').write_text(
        Path('pipeline.py').read_text().replace("CHECK_OUTPUTS = 'never'",
                                                "CHECK_OUTPUTS = 'fallback'"))
    cli('run', 'pipeline.py', '-E', 'slurm')
    gen = jobs(cluster)['gen'].jobid
    cluster.kill(gen, 2, 'TIMEOUT', partial=True)
    assert Path('data/gen_2.txt').exists()  # written before the kill
    cluster.drain()
    assert status('gen', n=2) == TASK_STATUS_PENDING
    assert planned(capsys) == 3


def test_a_result_already_in_is_not_marked_pending(cluster):
    # A task whose result was ingested before the mark keeps it.
    rmk = load_remake('pipeline.py')
    task = next(t for t in rmk.tasks() if t.rule.name == 'gen')
    rmk.metadata.update_task(task, TASK_STATUS_SUCCESS)
    seq = rmk.metadata.get_tasks_status([task])[task.key].run_seq
    rmk.metadata.mark_submitted([task], seq)
    assert rmk.metadata.get_tasks_status([task])[task.key].status == TASK_STATUS_SUCCESS
    rmk.metadata.mark_submitted([task], seq + 1)
    rec = rmk.metadata.get_tasks_status([task])[task.key]
    assert rec.status == TASK_STATUS_PENDING and rec.run_seq == seq  # run_seq kept


def test_dry_run_marks_nothing(cluster):
    cli('run', 'pipeline.py')
    cli('run', 'pipeline.py', '-E', 'slurm', '-f', '-n')
    assert status('gen', n=0) == TASK_STATUS_SUCCESS


def test_only_rules_actually_submitted_are_marked(cluster):
    # submit.sh stops at the first failing sbatch (set -e): here proc's,
    # whose dependency on a forgotten job is rejected.
    cli('run', 'pipeline.py')
    cluster.forget('1001')  # the next job id: gen's
    cli('run', 'pipeline.py', '-E', 'slurm', '-f')
    assert status('gen', n=0) == TASK_STATUS_PENDING
    assert status('proc', n=0) == TASK_STATUS_SUCCESS


def test_slurm_status_why_and_task_info_report_the_kill(cluster, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    gen = jobs(cluster)['gen'].jobid
    cluster.kill(gen, 1, 'NODE_FAIL')
    cluster.drain()
    # More than a minute after submission (slurm-status doesn't judge
    # "lost" before squeue can list a new job).
    for path in Path('.remake/jobs').glob('*.jobids.json'):
        rec = json.loads(path.read_text())
        path.write_text(json.dumps({**rec, 'submitted': rec['submitted'] - 120}))
    # slurm-status: lost = in the submission, not queued, no result.
    capsys.readouterr()
    cli('slurm-status', 'pipeline.py', '--json')
    rows = {r['rule']: r for r in json.loads(capsys.readouterr().out)['submissions']}
    assert (rows['gen']['lost'], rows['proc']['lost'], rows['agg']['lost']) == (1, 1, 1)
    # why: the cause, from sacct.
    rmk = load_remake('pipeline.py')
    key = next(t.key for t in rmk.tasks() if t.rule.name == 'gen' and t.kwargs == {'n': 1})
    proc_key = next(t.key for t in rmk.tasks()
                    if t.rule.name == 'proc' and t.kwargs == {'n': 1})
    cli('why', 'pipeline.py', key)
    out = capsys.readouterr().out
    assert f'SLURM job {gen}_1' in out and 'NODE_FAIL on host' in out
    cli('why', 'pipeline.py', proc_key)
    assert 'no accounting record (cancelled before it started' in capsys.readouterr().out
    cli('task-info', 'pipeline.py', key)
    assert 'NODE_FAIL on host' in capsys.readouterr().out


def test_queued_tasks_are_reported_from_squeue(cluster, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    gen = jobs(cluster)['gen'].jobid
    cluster.start(gen, 0)
    rmk = load_remake('pipeline.py')
    keys = {t.kwargs['n']: t.key for t in rmk.tasks() if t.rule.name == 'gen'}
    capsys.readouterr()
    cli('why', 'pipeline.py', keys[0])
    assert 'running (R)' in capsys.readouterr().out
    cli('why', 'pipeline.py', keys[1])
    assert 'queued (PD)' in capsys.readouterr().out
    assert not cluster.calls('sacct')  # squeue was enough


def test_sacct_failure_is_reported_not_raised(cluster, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    gen = jobs(cluster)['gen'].jobid
    cluster.kill(gen, 1, 'NODE_FAIL')
    cluster.fail('sacct')
    rmk = load_remake('pipeline.py')
    key = next(t.key for t in rmk.tasks() if t.rule.name == 'gen' and t.kwargs == {'n': 1})
    capsys.readouterr()
    cli('why', 'pipeline.py', key)
    assert 'not queued; sacct failed' in capsys.readouterr().out

