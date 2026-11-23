"""The DB side of reconcile (plan §3.5-3.6, step 4): the slurm_submission
table and the strict-guard killed-task write."""
from remake.loader import load_remake
from remake.metadata.metadata_manager import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from remake.remake_cmd import remake_cmd


def cli(*args):
    return remake_cmd(['remake', *args])


def key_of(rmk, rule, **kwargs):
    return next(t.key for t in rmk.tasks() if t.rule.name == rule and t.kwargs == kwargs)


def record(rmk, key):
    task = next(t for t in rmk.tasks() if t.key == key)
    return rmk.metadata.get_tasks_status([task]).get(key)


def test_killed_record_over_an_older_success(cluster):
    cli('run', 'pipeline.py')  # local: every task succeeds (run 1)
    rmk = load_remake('pipeline.py')
    key = key_of(rmk, 'gen', n=1)
    before = record(rmk, key)
    assert before.status == TASK_STATUS_SUCCESS
    meta = rmk.metadata
    applied = meta.record_killed('gen', key, before.run_seq + 1,
                                 'SlurmNodeFail: job 1001_1 on host1 after 00:13:40 (run 2)',
                                 '2026-11-23 17:05:02', host='host1', wall_s=820.0,
                                 reason='code-changed')
    assert applied
    after = record(rmk, key)
    assert after.status == TASK_STATUS_FAILED and after.run_seq == before.run_seq + 1
    # Code ids and the paths digest are those of the last run that finished.
    assert (after.run_code_id, after.uses_code_id, after.io_code_id, after.paths_digest) == (
        before.run_code_id, before.uses_code_id, before.io_code_id, before.paths_digest)
    task = next(t for t in rmk.tasks() if t.key == key)
    hist = meta.get_history(task)
    assert hist[0]['status'] == TASK_STATUS_FAILED and hist[0]['host'] == 'host1'
    assert hist[0]['error'].startswith('SlurmNodeFail')
    # A repeat (another reconciler) changes nothing: one record, one row.
    assert not meta.record_killed('gen', key, before.run_seq + 1, 'SlurmNodeFail: x',
                                  '2026-11-23 17:05:02')
    assert len(meta.get_history(task)) == len(hist)


def test_never_over_a_record_as_new_as_the_submission(cluster):
    cli('run', 'pipeline.py')
    rmk = load_remake('pipeline.py')
    key = key_of(rmk, 'gen', n=0)
    rec = record(rmk, key)
    assert not rmk.metadata.record_killed('gen', key, rec.run_seq, 'SlurmNodeFail: x',
                                          '2026-11-23 17:05:02')
    assert record(rmk, key).status == TASK_STATUS_SUCCESS


def test_a_late_real_result_replaces_the_killed_record(cluster):
    # The worker's sidecar lands after reconcile wrote the killed record.
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.sync()
    gen = next(j for j in cluster.jobs.values() if j.name == 'gen')
    rmk = load_remake('pipeline.py')
    key = key_of(rmk, 'gen', n=2)
    from remake.executors.slurm_executor import load_job_specs, spec_path, pointer_files
    import json
    run_seq = json.loads(pointer_files()['gen'].read_text())['run_seq']
    assert rmk.metadata.record_killed('gen', key, run_seq, 'SlurmNodeFail: x',
                                      '2026-11-23 17:05:02')
    cluster.finish(gen.jobid, 2)  # the real result, same run_seq
    rmk = load_remake('pipeline.py')
    rmk.metadata.ingest_sidecars(rmk.rules)
    assert record(rmk, key).status == TASK_STATUS_SUCCESS


def test_submissions_upsert(cluster):
    rmk = load_remake('pipeline.py')
    meta = rmk.metadata
    meta.record_submissions([('gen', 5, '1001', 4, 1_800_000_000)])
    meta.settle_submission('gen', 5, {'done': 4}, settled=True)
    (sub,) = meta.submissions()
    assert sub['reconciled'] and sub['counts'] == {'done': 4}
    # The same job again: still reconciled.
    meta.record_submissions([('gen', 5, '1001', 4, None)])
    assert meta.submissions(unreconciled=True) == []
    # `resubmit`: same run_seq, a new job id -> to be reconciled again.
    meta.record_submissions([('gen', 5, '1009', 4, None)])
    (sub,) = meta.submissions(unreconciled=True)
    assert sub['jobid'] == '1009' and sub['submitted'] == 1_800_000_000
    meta.settle_submission('gen', 5, {'unknown': 4}, settled=False)
    assert meta.submissions(unreconciled=True)[0]['counts'] == {'unknown': 4}


def test_unknown_rule_writes_nothing(cluster):
    rmk = load_remake('pipeline.py')
    assert not rmk.metadata.record_killed('gone', 'k' * 40, 3, 'x', '2026-11-23 00:00:00')


def test_status_by_key(cluster):
    cli('run', 'pipeline.py')
    rmk = load_remake('pipeline.py')
    key = key_of(rmk, 'gen', n=0)
    got = rmk.metadata.status_by_key([key, 'f' * 40])
    assert list(got) == [key] and got[key][0] == TASK_STATUS_SUCCESS
