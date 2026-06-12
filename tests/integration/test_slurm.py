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
from remake import MatrixNotReady, Remake, rule

@rule(outputs={'ids': 'data/ids.json'})
def discover(outputs):
    Path(outputs['ids']).write_text(json.dumps([1, 2, 3]))

def dyn_matrix():
    p = Path('data/ids.json')
    if not p.exists():
        raise MatrixNotReady(str(p))
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


# --- generation (dry run: writes everything, submits nothing) ---


def test_dry_run_writes_job_specs(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    specs = json.loads(Path('.remake/jobs/gen.json').read_text())
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
    assert 'remake run-array-task pipeline.py gen $SLURM_ARRAY_TASK_ID' in gen

    agg = Path('.remake/slurm/agg.sbatch').read_text()
    assert '--array' not in agg  # below threshold: individual job
    assert '#SBATCH --mem=8G' in agg  # rule config overrides Remake config
    assert '#SBATCH --partition=test-par' in agg
    assert 'remake run-array-task pipeline.py agg $1' in agg


def test_dry_run_writes_submit_sh_wiring(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    submit = Path('.remake/submit.sh').read_text()
    assert 'JOB_gen=$(sbatch --parsable .remake/slurm/gen.sbatch)' in submit
    # Same matrix, element-wise correspondence: aftercorr.
    assert (
        'JOB_proc=$(sbatch --parsable --dependency=aftercorr:$JOB_gen '
        '.remake/slurm/proc.sbatch)' in submit
    )
    # Fan-in: individual job, afterok on the whole upstream array.
    assert '--dependency=afterok:$JOB_proc' in submit
    assert '.remake/slurm/agg.sbatch 0' in submit
    # Sidecar writes.
    assert '> .remake/jobs/gen.jobids.json' in submit
    assert 'slurm_job_ids' in submit  # individual-job sidecar for agg
    assert 'continuation' not in submit  # no dynamic rules


# --- submission flow against the shims ---


def test_submission_writes_jobid_sidecars(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm')
    calls = sbatch_calls(slurm_dir)
    assert len(calls) == 3  # gen array, proc array, agg individual
    # Shell vars were expanded to the shim's job ids at submission time.
    assert '--dependency=aftercorr:1001' in calls[1]
    assert '--dependency=afterok:1002' in calls[2]
    assert json.loads(Path('.remake/jobs/gen.jobids.json').read_text()) == {
        'slurm_array_job_id': '1001'
    }
    assert json.loads(Path('.remake/jobs/agg.jobids.json').read_text()) == {
        'slurm_job_ids': ['1003']
    }


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


def test_resubmit_reexecutes_submit_sh(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm')
    nsubmitted = len(sbatch_calls(slurm_dir))
    cli('resubmit', 'pipeline.py')
    assert len(sbatch_calls(slurm_dir)) == nsubmitted * 2


def test_resubmit_without_submit_sh_errors(slurm_dir):
    from remake import RemakeError

    with pytest.raises(RemakeError, match='No .remake/submit.sh'):
        cli('resubmit', 'pipeline.py')


# --- run-array-task: the payload SLURM jobs execute ---


def test_run_array_task_executes_spec(slurm_dir):
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    cli('run-array-task', 'pipeline.py', 'gen', '3')
    assert Path('data/gen_3.txt').read_text() == '3'
    # And it is recorded: a replan no longer includes gen[n=3].
    cli('run', 'pipeline.py', '-E', 'slurm', '--dry-run')
    specs = json.loads(Path('.remake/jobs/gen.json').read_text())
    assert {'n': 3} not in [spec['kwargs'] for spec in specs]


# --- dynamic matrices: continuation job ---


def test_deferred_rule_gets_continuation_job(slurm_dir):
    Path('dynamic.py').write_text(DYNAMIC_PIPELINE)
    cli('run', 'dynamic.py', '-E', 'slurm', '--dry-run')
    assert not Path('.remake/jobs/dyn.json').exists()  # deferred: no spec yet
    submit = Path('.remake/submit.sh').read_text()
    assert (
        'sbatch --parsable --dependency=afterok:$JOB_discover_0 '
        '.remake/slurm/continuation.sbatch' in submit
    )
    continuation = Path('.remake/slurm/continuation.sbatch').read_text()
    assert 'remake run dynamic.py --executor slurm' in continuation
    assert '#SBATCH --partition=test-par' in continuation


def test_slurm_executor_needs_remakefile():
    from remake import Remake, RemakeError, SlurmExecutor

    with pytest.raises(RemakeError, match='remakefile'):
        SlurmExecutor(Remake())
