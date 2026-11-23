"""Per-submission records (plan step 2): jobids files that later
submissions don't overwrite, <jobid>_<index> output names, pruning."""
import json
import os
import time
from pathlib import Path

from remake.executors.slurm_executor import (
    element_output,
    pointer_files,
    prune_spec_files,
    submission_files,
)
from remake.remake_cmd import remake_cmd


def cli(*args):
    return remake_cmd(['remake', *args])


def test_a_resubmission_keeps_the_earlier_record(cluster):
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.sync()
    gen1 = next(j for j in cluster.jobs.values() if j.name == 'gen')
    cluster.kill(gen1.jobid, 1, 'NODE_FAIL')
    cluster.drain()
    cli('run', 'pipeline.py', '-E', 'slurm')  # the run-82 case: resubmits gen
    records = submission_files()
    runs = sorted(seq for rule, seq in records if rule == 'gen')
    assert len(runs) == 2
    first = json.loads(records[('gen', runs[0])].read_text())
    assert first['slurm_array_job_id'] == gen1.jobid and first['run_seq'] == runs[0]
    # The pointer names the latest.
    latest = json.loads(pointer_files()['gen'].read_text())
    assert latest['run_seq'] == runs[1] and latest['slurm_array_job_id'] != gen1.jobid


def test_outputs_are_named_by_job_and_index(cluster):
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.sync()
    gen = next(j for j in cluster.jobs.values() if j.name == 'gen')
    cluster.start(gen.jobid, 2)
    out = element_output('gen', gen.jobid, 2, 'out')
    assert out.name == f'{gen.jobid}_2.out' and out.exists()


def test_element_output_uses_old_names_only_for_old_submissions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = Path('.remake/slurm/output/gen')
    d.mkdir(parents=True)
    (d / '3.err').write_text('boom')
    # A pre-0.11 submission's file...
    assert element_output('gen', '1001', 3, 'err', legacy=True) == d / '3.err'
    # ...is never taken for a 0.11 submission's (it may be another task's).
    assert element_output('gen', '1001', 3, 'err') == d / '1001_3.err'


def test_resubmit_guard_finds_rules_in_a_quoted_path(tmp_path, monkeypatch):
    # A metadata dir with a space: shlex.quote adds quotes, and the guard
    # must still see which rules the script submits (else it passes
    # silently, the unsafe way).

    from remake.executors import slurm_executor as se

    text = ("mv '/a b/.remake/jobs/gen.3.jobids.tmp' '/a b/.remake/jobs/gen.jobids.json'\n"
            "echo x > .remake/jobs/proc.jobids.json\n")
    calls = []
    monkeypatch.setattr(se, 'active_jobids', lambda: {'1001'})
    monkeypatch.setattr(se, 'recorded_jobids', lambda rule: calls.append(rule) or ['1001'])
    p = tmp_path / 'submit.sh'
    p.write_text(text)
    import pytest

    from remake.core.exceptions import RemakeError
    with pytest.raises(RemakeError, match='Still queued'):
        se.check_resubmit_safe(p)
    assert calls == ['gen', 'proc']


def test_pruning_takes_old_records_and_outputs_but_keeps_the_latest(cluster):
    cli('run', 'pipeline.py', '-E', 'slurm')
    cluster.drain()
    cli('run', 'pipeline.py', '-E', 'slurm', '-f', '-Q', "rule == 'gen'")
    cluster.drain()
    old = time.time() - 30 * 86400
    for path in list(Path('.remake/jobs').iterdir()) + list(
            Path('.remake/slurm/output').glob('*/*')):
        os.utime(path, (old, old))
    prune_spec_files()
    left = submission_files()
    latest = {rule: json.loads(p.read_text())['run_seq'] for rule, p in pointer_files().items()}
    # Only each rule's latest submission record is left...
    assert set(left) == {(rule, seq) for rule, seq in latest.items()}
    # ...and only the outputs of the latest jobs.
    keep = {json.loads(p.read_text())['slurm_array_job_id'] for p in pointer_files().values()}
    assert {p.name.split('_')[0] for p in Path('.remake/slurm/output').glob('*/*_*')} <= keep
