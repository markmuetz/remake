"""SLURM executor tests — generation is asserted on file content; the
submission flow runs against fake sbatch/squeue shims on PATH, so the whole
flow short of real cluster behaviour is covered locally."""
import json
import os
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd

# Logs its args and prints an incrementing job id (1001, 1002, ...), like
# sbatch --parsable.
SBATCH_SHIM = '''#!/bin/bash
dir="$(dirname "$0")"
echo "$@" >> "$dir/sbatch.log"
n=$(cat "$dir/jobid_counter" 2>/dev/null || echo 1000)
n=$((n + 1))
echo $n > "$dir/jobid_counter"
echo $n
'''

# Prints canned output (written by tests), empty by default.
SQUEUE_SHIM = '''#!/bin/bash
dir="$(dirname "$0")"
cat "$dir/squeue.out" 2>/dev/null
exit 0
'''

# A broken squeue (controller down/throttled): errors instead of reporting.
SQUEUE_FAIL_SHIM = '''#!/bin/bash
echo 'slurm_load_jobs error: Socket timed out on send/recv operation' >&2
exit 1
'''


def break_squeue(slurm_dir):
    (slurm_dir / 'shim/squeue').write_text(SQUEUE_FAIL_SHIM)

# gen/proc: 12 tasks, same matrix -> arrays wired with aftercorr.
# agg: fan-in, 1 task -> individual job with afterok; per-rule config merge.
PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/gen_{n}.txt'}, matrix={'n': list(range(12))})
def gen(outputs, n):
    Path(outputs['o']).write_text(str(n))

@rule(inputs=gen.outputs, outputs={'o': 'data/proc_{n}.txt'},
      matrix=gen.matrix, depends_on=[gen])
def proc(inputs, outputs, n):
    Path(outputs['o']).write_text(Path(inputs['o']).read_text() * 2)

def agg_inputs():
    return {str(n): f'data/proc_{n}.txt' for n in range(12)}

@rule(inputs=agg_inputs, outputs={'o': 'data/agg.txt'}, depends_on=[proc],
      config={'slurm': {'mem': '8G'}})
def agg(inputs, outputs):
    parts = [Path(p).read_text() for p in inputs.values()]
    Path(outputs['o']).write_text(','.join(parts))

rmk = Remake(config={'slurm': {'partition': 'test-par', 'mem': '2G'}})
rmk.rules_from_current_module()
'''

DYNAMIC_PIPELINE = '''
import json
from pathlib import Path
from remake import Defer, Remake, deferrable, rule

@rule(outputs={'ids': 'data/ids.json'})
def discover(outputs):
    Path(outputs['ids']).write_text(json.dumps([1, 2, 3]))

@deferrable
def dyn_matrix():
    p = Path('data/ids.json')
    if not p.exists():
        raise Defer(str(p))
    return [{'i': i} for i in json.loads(p.read_text())]

@rule(outputs={'o': 'data/dyn_{i}.txt'}, matrix=dyn_matrix, depends_on=[discover])
def dyn(outputs, i):
    Path(outputs['o']).write_text(str(i))

rmk = Remake(config={'slurm': {'partition': 'test-par'}})
rmk.rules_from_current_module()
'''


def cli(*args):
    return remake_cmd(['remake', *args])


@pytest.fixture
def slurm_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    shim = tmp_path / 'shim'
    shim.mkdir()
    for name, text in [('sbatch', SBATCH_SHIM), ('squeue', SQUEUE_SHIM)]:
        path = shim / name
        path.write_text(text)
        path.chmod(0o755)
    monkeypatch.setenv('PATH', f'{shim}{os.pathsep}{os.environ["PATH"]}')
    Path('pipeline.py').write_text(PIPELINE)
    return tmp_path


def sbatch_calls(slurm_dir):
    log = slurm_dir / 'shim/sbatch.log'
    return log.read_text().splitlines() if log.exists() else []


def specs_file(rule):
    """The rule's most recent job-spec file (they are per-submission,
    .remake/jobs/<rule>.<run_seq>.json)."""
    from remake.executors.slurm_executor import latest_spec_path
    return latest_spec_path(rule)


def read_specs(rule):
    return json.loads(specs_file(rule).read_text())


# --- generation (dry run: writes everything, submits nothing) ---


def test_dry_run_writes_job_specs(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    assert specs_file('gen').match('.remake/jobs/gen.*.json')
    specs = read_specs('gen')
    assert len(specs) == 12
    assert specs[3]['rule'] == 'gen'
    assert specs[3]['kwargs'] == {'n': 3}
    assert len(specs[3]['task_key']) == 40
    assert not sbatch_calls(slurm_dir)


def test_dry_run_writes_sbatch_scripts(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    gen = Path('.remake/slurm/gen.sbatch').read_text()
    assert '#SBATCH --array=0-11' in gen
    assert '#SBATCH --partition=test-par' in gen
    assert '#SBATCH --mem=2G' in gen
    # Cancel (don't indefinitely park) elements whose upstream dependency fails.
    assert '#SBATCH --kill-on-invalid-dep=yes' in gen
    # The script pins its own submission's (immutable) spec file, so later
    # replans can't change what a queued array executes.
    assert (
        'remake run-array-task pipeline.py gen $SLURM_ARRAY_TASK_ID '
        f'--specs {specs_file("gen")}' in gen
    )
    # The wrapper must propagate the task's exit code, not mask it with the
    # trailing echo -- otherwise SLURM sees every element as exit 0 and
    # aftercorr/afterok never block dependants of a failed task.
    assert 'rc=$?' in gen
    assert 'exit $rc' in gen

    agg = Path('.remake/slurm/agg.sbatch').read_text()
    assert '#SBATCH --array=0-0' in agg  # single task: still an array job
    assert '#SBATCH --kill-on-invalid-dep=yes' in agg
    assert 'rc=$?' in agg and 'exit $rc' in agg
    assert '#SBATCH --mem=8G' in agg  # rule config overrides Remake config
    assert '#SBATCH --partition=test-par' in agg
    assert (
        'remake run-array-task pipeline.py agg $SLURM_ARRAY_TASK_ID '
        f'--specs {specs_file("agg")}' in agg
    )


def test_dry_run_writes_submit_sh_wiring(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    submit = Path('.remake/submit.sh').read_text()
    assert 'JOB_gen=$(sbatch --parsable .remake/slurm/gen.sbatch)' in submit
    # Same matrix, element-wise correspondence: aftercorr.
    assert (
        'JOB_proc=$(sbatch --parsable --dependency=aftercorr:$JOB_gen '
        '.remake/slurm/proc.sbatch)' in submit
    )
    # Fan-in: afterok on the whole upstream array (not element-wise).
    assert (
        'JOB_agg=$(sbatch --parsable --dependency=afterok:$JOB_proc '
        '.remake/slurm/agg.sbatch)' in submit
    )
    # Sidecar writes: one array-form sidecar per rule.
    assert '> .remake/jobs/gen.jobids.json' in submit
    assert '> .remake/jobs/agg.jobids.json' in submit
    assert 'slurm_array_job_id' in submit and 'slurm_job_ids' not in submit
    assert 'continuation' not in submit  # no dynamic rules


# --- submission flow against the shims ---


def test_submission_writes_jobid_sidecars(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm')
    calls = sbatch_calls(slurm_dir)
    assert len(calls) == 3  # gen array, proc array, agg individual
    # Shell vars were expanded to the shim's job ids at submission time.
    assert '--dependency=aftercorr:1001' in calls[1]
    assert '--dependency=afterok:1002' in calls[2]
    # Sidecars record run_seq, pinning the jobids to the spec file they
    # were submitted with.
    gen_sidecar = json.loads(Path('.remake/jobs/gen.jobids.json').read_text())
    agg_sidecar = json.loads(Path('.remake/jobs/agg.jobids.json').read_text())
    run_seq = read_specs('gen')[0]['run_seq']
    assert gen_sidecar == {'slurm_array_job_id': '1001', 'run_seq': run_seq}
    assert agg_sidecar == {'slurm_array_job_id': '1003', 'run_seq': run_seq}
    assert specs_file('gen').name == f'gen.{run_seq}.json'


def test_already_queued_rule_is_skipped(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    # gen's array job (1001) still has queued/running elements.
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 PD\n1001_4 R\n')

    cli('run', 'pipeline.py', '-E', 'slurm')
    calls = sbatch_calls(slurm_dir)
    assert len(calls) == nsubmitted + 2  # proc + agg resubmitted, gen skipped
    # Downstream of a queued rule: afterok on the literal queued job id
    # (aftercorr is unsafe against a previous submission's array).
    assert '--dependency=afterok:1001' in Path('.remake/submit.sh').read_text()
    assert not any('gen.sbatch' in call for call in calls[nsubmitted:])


def test_stencil_rule_gets_afterok_not_aftercorr(slurm_dir):
    # Review finding 7: equal matrices do not imply element-wise dependence.
    # A stencil rule (task n reads upstream n-1 and n) has the same matrix
    # as its upstream, but aftercorr would start element n when upstream
    # element n alone finishes — reading a not-yet-written n-1 output,
    # silent partial data. Correspondence must be proved from task
    # inputs/outputs, falling back to afterok.
    Path('stencil.py').write_text('''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/gen_{n}.txt'}, matrix={'n': list(range(12))})
def gen(outputs, n):
    Path(outputs['o']).write_text(str(n))

def smooth_inputs(n):
    return {'lo': f'data/gen_{max(n - 1, 0)}.txt', 'hi': f'data/gen_{n}.txt'}

@rule(inputs=smooth_inputs, outputs={'o': 'data/smooth_{n}.txt'},
      matrix={'n': list(range(12))}, depends_on=[gen])
def smooth(inputs, outputs, n):
    Path(outputs['o']).write_text('x')

rmk = Remake()
rmk.rules_from_current_module()
''')
    cli('run', 'stencil.py', '-E', 'slurm', '--dry-run')
    submit = Path('.remake/submit.sh').read_text()
    assert '--dependency=afterok:$JOB_gen' in submit
    assert 'aftercorr' not in submit


def test_suspended_job_still_counts_as_queued(slurm_dir):
    # Review finding 3: the active filter used to accept only PD/R/CF, so a
    # suspended array (scontrol suspend, gang preemption) looked done and
    # was double-submitted. Active is now "any non-terminal state".
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 S\n')

    cli('run', 'pipeline.py', '-E', 'slurm')
    calls = sbatch_calls(slurm_dir)
    assert not any('gen.sbatch' in call for call in calls[nsubmitted:])


def test_terminal_job_state_does_not_block_resubmission(slurm_dir):
    # The other side of the inverted filter: a job squeue still lists but in
    # a terminal state (completed, briefly visible) must not park the rule.
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 CD\n1001_4 F\n')

    cli('run', 'pipeline.py', '-E', 'slurm')
    calls = sbatch_calls(slurm_dir)
    assert any('gen.sbatch' in call for call in calls[nsubmitted:])


def test_dry_run_does_not_overwrite_queued_rule_specs(slurm_dir):
    # todos.md filed --dry-run as bypassing the already-queued guard and
    # overwriting a queued rule's specs while array elements still read
    # them. The guard runs before spec-writing on both paths (dry_run only
    # skips submit()), so a dry run must leave a queued rule's specs alone.
    cli('run', 'pipeline.py', '-E', 'slurm')
    submitted = specs_file('gen')
    specs_before = submitted.read_text()
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 PD\n1001_4 R\n')

    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run', '--force')
    assert specs_file('gen') == submitted  # skipped: no new spec file either
    assert submitted.read_text() == specs_before
    # Non-queued downstream rules still get fresh specs written.
    assert '--dependency=afterok:1001' in Path('.remake/submit.sh').read_text()


def test_replan_writes_new_spec_file_leaving_old_intact(slurm_dir):
    # The invariant per-submission spec files exist for: no code path ever
    # rewrites an existing spec file — a replan (here with the previous jobs
    # gone from the queue, the case the already-queued guard can't help
    # with) writes a fresh file under its own run_seq, and each sbatch
    # script pins its own submission's file via --specs. A previously
    # queued array therefore executes the exact task list it was submitted
    # with, no matter what happens afterwards.
    cli('run', 'pipeline.py', '-E', 'slurm')
    first = specs_file('gen')
    first_content = first.read_text()

    cli('run', 'pipeline.py', '-E', 'slurm')  # queue empty: full resubmit
    second = specs_file('gen')
    assert second != first
    assert first.read_text() == first_content
    assert f'--specs {second}' in Path('.remake/slurm/gen.sbatch').read_text()


def test_resubmit_reexecutes_submit_sh(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    cli('resubmit', 'pipeline.py')
    assert len(sbatch_calls(slurm_dir)) == nsubmitted * 2


def test_resubmit_without_submit_sh_errors(slurm_dir, capsys):
    # User-facing error: caught by the CLI as exit 2 + clean message.
    assert cli('resubmit', 'pipeline.py') == 2
    assert 'No .remake/submit.sh' in capsys.readouterr().err


def test_resubmit_refuses_while_jobs_still_queued(slurm_dir, capsys):
    # Review finding 6: resubmit used to re-execute submit.sh with no queue
    # check, submitting duplicates of still-queued rules (two writers per
    # output) and overwriting their sidecars.
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 PD\n')

    assert cli('resubmit', 'pipeline.py') == 2
    err = capsys.readouterr().err
    assert 'Still queued' in err and 'gen' in err and '1001' in err
    assert len(sbatch_calls(slurm_dir)) == nsubmitted  # nothing re-executed


def test_resubmit_refuses_when_squeue_fails(slurm_dir, capsys):
    # Resubmit executes submit.sh verbatim; with the queue unknowable the
    # only safe course is to refuse.
    cli('run', 'pipeline.py', '-E', 'slurm')
    break_squeue(slurm_dir)
    assert cli('resubmit', 'pipeline.py') == 2
    assert 'refusing to resubmit' in capsys.readouterr().err


def test_resubmit_refuses_stale_literal_dependency_ids(slurm_dir, capsys):
    # A rule skipped as already-queued wires downstream --dependency flags
    # to its literal job ids. Once those jobs leave the queue, sbatch
    # rejects the dependency and set -e aborts submit.sh halfway — refuse
    # and point at a replan instead.
    cli('run', 'pipeline.py', '-E', 'slurm')
    (slurm_dir / 'shim/squeue.out').write_text('1001_3 PD\n')
    cli('run', 'pipeline.py', '-E', 'slurm')  # gen skipped: afterok:1001 baked in
    assert '--dependency=afterok:1001' in Path('.remake/submit.sh').read_text()
    (slurm_dir / 'shim/squeue.out').write_text('')  # 1001 gone from the queue
    nsubmitted = len(sbatch_calls(slurm_dir))

    assert cli('resubmit', 'pipeline.py') == 2
    err = capsys.readouterr().err
    assert '1001' in err and 'left the queue' in err
    assert len(sbatch_calls(slurm_dir)) == nsubmitted


# --- run-array-task: the payload SLURM jobs execute ---


def test_run_array_task_writes_per_task_log(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    shared = {p: Path(p).read_text() for p in
              ('.remake/remake.log', '.remake/remake.debug.log',
               '.remake/remake.jsonl')}

    cli('run-array-task', 'pipeline.py', 'gen', '3')
    key = read_specs('gen')[3]['task_key']
    task_log = Path(f'.remake/tasks/log/gen/{key[:2]}/{key[2:]}.log')
    assert 'gen[n=3]' in task_log.read_text()
    # The shared logs are untouched: per-task processes must not append to
    # any of them (concurrent corruption, see design_docs/per_task_logging.md).
    for path, before in shared.items():
        assert Path(path).read_text() == before, path

    # Overwrite, not append: rerunning leaves one attempt in the file.
    cli('run-array-task', 'pipeline.py', 'gen', '3')
    assert task_log.read_text().count('Running') == 1


def test_run_array_task_executes_spec(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    cli('run-array-task', 'pipeline.py', 'gen', '3')
    assert Path('data/gen_3.txt').read_text() == '3'
    # And it is recorded: a replan no longer includes gen[n=3].
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    specs = read_specs('gen')
    assert {'n': 3} not in [spec['kwargs'] for spec in specs]


def test_run_array_task_writes_sidecar_not_db(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    db_before = Path('.remake/remake.db').read_bytes()

    cli('run-array-task', 'pipeline.py', 'gen', '3')
    # The array process must not touch the shared DB (livelock on shared
    # filesystems); the result goes to a sidecar instead.
    assert Path('.remake/remake.db').read_bytes() == db_before
    spec = read_specs('gen')[3]
    key = spec['task_key']
    sidecar = Path(f'.remake/tasks/results/gen/{key[:2]}/{key[2:]}.json')
    payload = json.loads(sidecar.read_text())
    assert payload['status'] == 1  # TASK_STATUS_SUCCESS
    assert 'uses_hash' in payload and 'timestamp' in payload
    # run_seq is fixed at submission (in the job spec) and carried into the
    # sidecar so durable propagation survives the submit->compute boundary.
    assert spec['run_seq'] is not None and payload['run_seq'] == spec['run_seq']

    # The next planning invocation ingests: sidecar gone, task complete.
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    assert not sidecar.exists()
    specs = read_specs('gen')
    assert {'n': 3} not in [spec['kwargs'] for spec in specs]


def test_ingest_after_edit_detects_code_change(slurm_dir):
    # Bug 05: the sidecar must carry the run source that executed, so that
    # ingesting under *edited* source (run overnight, tweak in the morning)
    # still detects the code change and replans the task.
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    cli('run-array-task', 'pipeline.py', 'gen', '3')
    key = read_specs('gen')[3]['task_key']
    sidecar = Path(f'.remake/tasks/results/gen/{key[:2]}/{key[2:]}.json')
    payload = json.loads(sidecar.read_text())
    assert payload['run_hash']  # what actually ran travels in the sidecar

    # Edit the rule body before anything ingests the sidecar.
    Path('pipeline.py').write_text(PIPELINE.replace(
        "Path(outputs['o']).write_text(str(n))",
        "Path(outputs['o']).write_text(str(n) + '!')",
    ))

    # This invocation ingests the sidecar, then plans: gen[n=3] completed
    # under the old code, so the edit must put it back in the plan.
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    assert not sidecar.exists()
    specs = read_specs('gen')
    assert {'n': 3} in [spec['kwargs'] for spec in specs]


def test_failed_sidecar_traceback_reaches_info(slurm_dir, capsys):
    Path('failing.py').write_text('''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/f_{n}.txt'}, matrix={'n': [1, 2]})
def f(outputs, n):
    if n == 2:
        raise ValueError('boom from n=2')
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
''')
    cli('run', 'failing.py', '-E', 'slurm', '--dry-run')
    with pytest.raises(ValueError):
        cli('run-array-task', 'failing.py', 'f', '1')  # the n=2 spec

    capsys.readouterr()
    cli('info', 'failing.py', '-F')  # ingests, then reports
    out = capsys.readouterr().out
    assert 'ValueError: boom from n=2' in out
    assert not list(Path('.remake/tasks/results').rglob('*.json'))


def test_slurm_status_reports_queue_state(slurm_dir, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    (slurm_dir / 'shim/squeue.out').write_text(
        '1001_3 PD Dependency\n'
        '1001_4 R None\n'
        '1003 PD DependencyNeverSatisfied\n'
    )
    capsys.readouterr()
    cli('slurm-status', 'pipeline.py')
    out = capsys.readouterr().out
    assert 'PD:1 R:1' in out and '[Dependency]' in out  # gen, job 1001
    assert 'not in queue' in out  # proc, job 1002, no squeue rows
    assert 'DependencyNeverSatisfied' in out  # agg, job 1003

    cli('slurm-status', 'pipeline.py', '--json')
    rows = json.loads(capsys.readouterr().out)
    by_rule = {row['rule']: row for row in rows}
    assert by_rule['gen']['jobid'] == '1001' and by_rule['gen']['states'] == {'PD': 1, 'R': 1}
    assert by_rule['agg']['reasons'] == ['DependencyNeverSatisfied']


def test_task_info_shows_slurm_submission(slurm_dir, capsys):
    cli('run', 'pipeline.py', '-E', 'slurm')
    capsys.readouterr()
    cli('task-info', 'pipeline.py', '-Q', 'rule == "gen" and n == 3')
    out = capsys.readouterr().out
    assert 'slurm:    job 1001' in out and 'array index 3' in out


def test_run_array_task_default_specs_ignore_dry_run(slurm_dir):
    # Without --specs, run-array-task must resolve the last SUBMITTED spec
    # file (via the jobids sidecar's run_seq), not the newest on disk: a
    # dry run writes a fresh spec file that no queued job is running, and a
    # manual retry of a failed element must mean the element that failed.
    cli('run', 'pipeline.py', '-E', 'slurm')
    cli('run-array-task', 'pipeline.py', 'gen', '3')  # completes gen[n=3]
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')  # ingest + replan
    assert {'n': 5} == read_specs('gen')[4]['kwargs']  # dry-run indices shifted

    cli('run-array-task', 'pipeline.py', 'gen', '4')
    # Element 4 of the submitted array is gen[n=4] — not the dry-run plan's
    # index 4 (n=5).
    assert Path('data/gen_4.txt').read_text() == '4'
    assert not Path('data/gen_5.txt').exists()


def test_task_info_array_index_survives_replan(slurm_dir, capsys):
    # The jobids sidecar records its submission's run_seq, so task-info maps
    # array indices against the spec file that was actually submitted — not
    # whichever replan wrote specs last. Complete gen[n=3] and replan: the
    # new spec file omits n=3 and shifts every later index down by one, but
    # gen[n=4] must still be attributed to element 4 of job 1001.
    cli('run', 'pipeline.py', '-E', 'slurm')
    cli('run-array-task', 'pipeline.py', 'gen', '3')  # completes gen[n=3]
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')  # ingest + replan
    assert {'n': 4} == read_specs('gen')[3]['kwargs']  # indices shifted

    capsys.readouterr()
    cli('task-info', 'pipeline.py', '-Q', 'rule == "gen" and n == 4')
    out = capsys.readouterr().out
    assert 'slurm:    job 1001' in out and 'array index 4' in out


def test_week_old_spec_files_pruned_except_last_submitted(slurm_dir):
    # Spec files accumulate one per rule per submission/dry-run; prune any
    # older than a week — except each rule's sidecar-referenced spec, which
    # a long-pending (walltime only bounds *run* time) last submission may
    # still read. See slurm_already_running.md, 2026-07-10 decisions.
    import time

    cli('run', 'pipeline.py', '-E', 'slurm')
    submitted = specs_file('gen')
    orphan = Path('.remake/jobs/gen.0.json')  # e.g. an ancient dry-run plan
    orphan.write_text('[]')
    week_plus = time.time() - 8 * 86400
    os.utime(orphan, (week_plus, week_plus))
    os.utime(submitted, (week_plus, week_plus))  # old but sidecar-referenced

    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    assert not orphan.exists()
    assert submitted.exists()
    assert specs_file('proc').exists()  # recent + unreferenced: untouched


def test_run_array_task_rejects_key_mismatch(slurm_dir, capsys):
    # Review finding 5's live half (matrix-sourced non-scalars are already
    # rejected at plan time, dag._check_scalar_kwargs): if a spec's kwargs
    # rebuild to a task whose key differs from the submitted one
    # (hand-edited or corrupt spec file), fail loudly instead of recording
    # the result under a phantom key the planner never reads.
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    path = specs_file('gen')
    specs = json.loads(path.read_text())
    specs[3]['kwargs'] = {'n': 99}  # no longer matches task_key
    path.write_text(json.dumps(specs))

    assert cli('run-array-task', 'pipeline.py', 'gen', '3', '--specs', str(path)) == 2
    assert 'rebuilt task key' in capsys.readouterr().err
    assert not Path('data').exists()  # nothing ran


def test_remakefile_with_space_is_quoted_in_scripts(slurm_dir):
    # Review finding 11: the remakefile is a user-typed name interpolated
    # into bash; unquoted, a space word-splits on the compute node and every
    # job fails after queueing.
    Path('my pipeline.py').write_text(PIPELINE)
    cli('run', 'my pipeline.py', '-E', 'slurm', '--dry-run')
    gen = Path('.remake/slurm/gen.sbatch').read_text()
    assert "remake run-array-task 'my pipeline.py' gen $SLURM_ARRAY_TASK_ID" in gen


def test_squeue_snapshot_timeout_raises(slurm_dir, monkeypatch):
    # A hung squeue (wedged controller connection) must fail safe like an
    # erroring one, not block the run forever.
    import remake.executors.slurm_executor as se

    (slurm_dir / 'shim/squeue').write_text('#!/bin/bash\nsleep 5\n')
    monkeypatch.setattr(se, 'SQUEUE_TIMEOUT', 0.2)
    with pytest.raises(se.SqueueError, match='no response'):
        se.squeue_snapshot()


# --- squeue failure: never mistaken for an empty queue ---


def test_squeue_failure_with_recorded_submissions_refuses(slurm_dir, capsys):
    # The latent resubmit-all bug (design_docs/slurm_already_running.md):
    # squeue failing used to look like an empty queue, green-lighting
    # duplicates of still-queued arrays. With submissions on record and the
    # queue state unknown, refuse to submit.
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    submitted = specs_file('gen')
    break_squeue(slurm_dir)

    assert cli('run', 'pipeline.py', '-E', 'slurm') == 2
    err = capsys.readouterr().err
    assert 'refusing to submit' in err
    assert 'gen' in err and 'Socket timed out' in err
    assert len(sbatch_calls(slurm_dir)) == nsubmitted  # nothing submitted
    assert specs_file('gen') == submitted  # no new spec files either


def test_squeue_failure_dry_run_also_refuses(slurm_dir, capsys):
    # A dry run stages submit.sh (which `remake resubmit` executes blind),
    # so it must not plan around an unknown queue either.
    cli('run', 'pipeline.py', '-E', 'slurm')
    break_squeue(slurm_dir)
    assert cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run') == 2
    assert 'refusing to submit' in capsys.readouterr().err


def test_squeue_failure_fresh_dir_proceeds(slurm_dir):
    # No jobids sidecars -> nothing we submitted can be queued: a broken or
    # missing squeue (e.g. planning off-cluster) must not block the first
    # submission.
    break_squeue(slurm_dir)
    cli('run', 'pipeline.py', '-E', 'slurm')
    assert len(sbatch_calls(slurm_dir)) == 3


def test_squeue_failure_slurm_status_errors_cleanly(slurm_dir, capsys):
    # slurm-status must report the failure, not "not in queue" for every job.
    cli('run', 'pipeline.py', '-E', 'slurm')
    break_squeue(slurm_dir)
    capsys.readouterr()
    assert cli('slurm-status', 'pipeline.py') == 2
    captured = capsys.readouterr()
    assert 'squeue failed' in captured.err
    assert 'not in queue' not in captured.out


def test_squeue_snapshot_missing_squeue_raises(tmp_path, monkeypatch):
    from remake.executors.slurm_executor import SqueueError, squeue_snapshot

    monkeypatch.setenv('PATH', str(tmp_path))  # no squeue anywhere
    with pytest.raises(SqueueError, match='squeue not found'):
        squeue_snapshot()


# --- dynamic matrices: continuation job ---


def test_deferred_rule_gets_continuation_job(slurm_dir):
    Path('dynamic.py').write_text(DYNAMIC_PIPELINE)
    cli('run', 'dynamic.py', '-E', 'slurm', '--dry-run')
    assert specs_file('dyn') is None  # deferred: no spec yet
    submit = Path('.remake/submit.sh').read_text()
    assert (
        'sbatch --parsable --dependency=afterok:$JOB_discover '
        '.remake/slurm/continuation.sbatch' in submit
    )
    continuation = Path('.remake/slurm/continuation.sbatch').read_text()
    assert 'remake run dynamic.py --executor slurm' in continuation
    assert '#SBATCH --partition=test-par' in continuation
    # Finding 9: without this, one failed upstream task leaves the
    # continuation pending forever as DependencyNeverSatisfied.
    assert '#SBATCH --kill-on-invalid-dep=yes' in continuation


def test_no_continuation_when_nothing_to_wait_on(slurm_dir, capsys):
    # Review finding 8: run_tasks([], deferred) used to submit a
    # dependency-less continuation that replans the same state and submits
    # another continuation, forever. If nothing was submitted or queued,
    # whatever the deferred matrices wait for cannot appear — warn instead.
    Path('dynamic.py').write_text(DYNAMIC_PIPELINE)
    cli('run', 'dynamic.py', '-E', 'slurm')
    cli('run-array-task', 'dynamic.py', 'discover', '0')  # writes data/ids.json
    Path('data/ids.json').unlink()  # discover recorded complete, dyn re-defers
    nsubmitted = len(sbatch_calls(slurm_dir))

    cli('run', 'dynamic.py', '-E', 'slurm')
    assert 'continuation' not in Path('.remake/submit.sh').read_text()
    assert len(sbatch_calls(slurm_dir)) == nsubmitted  # no self-replication


def test_slurm_executor_needs_remakefile():
    from remake import Remake, RemakeError, SlurmExecutor

    with pytest.raises(RemakeError, match='remakefile'):
        SlurmExecutor(Remake())
