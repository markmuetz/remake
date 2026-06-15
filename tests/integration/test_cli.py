import json
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd
from remake.version import __version__

PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'raw': 'data/raw_{n}.txt'}, matrix={'n': [1, 2]})
def generate(outputs, n):
    Path(outputs['raw']).write_text(str(n))

@rule(inputs=generate.outputs, outputs={'out': 'data/out_{n}.txt'},
      matrix=generate.matrix, depends_on=[generate])
def process(inputs, outputs, n):
    Path(outputs['out']).write_text(Path(inputs['raw']).read_text() * 2)

rmk = Remake()
rmk.rules_from_current_module()
'''


@pytest.fixture
def pipeline_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text(PIPELINE)
    return tmp_path


def cli(*args):
    return remake_cmd(['remake', *args])


def test_version(capsys):
    cli('version')
    assert capsys.readouterr().out.strip() == __version__


def test_no_subcommand_prints_help(capsys):
    assert cli() == 1
    assert 'usage:' in capsys.readouterr().out


def test_dry_run_runs_nothing(pipeline_dir, capsys):
    cli('run', 'pipeline.py', '--dry-run')
    out = capsys.readouterr().out
    assert '4 task(s) would run' in out
    assert not (pipeline_dir / 'data').exists()


def test_run_and_info(pipeline_dir, capsys):
    assert cli('run', 'pipeline.py') == 0
    assert (pipeline_dir / 'data/out_2.txt').read_text() == '22'

    cli('info', 'pipeline.py')
    out = capsys.readouterr().out
    lines = [line.split() for line in out.splitlines() if line.strip()]
    header_idx = next(i for i, line in enumerate(lines) if line[0] == 'rule')
    assert lines[header_idx][:3] == ['rule', 'tasks', 'success']
    by_rule = {line[0]: line[1:] for line in lines[header_idx + 1:]}
    assert by_rule['generate'] == ['2', '2', '0', '0', '0']
    assert by_rule['process'] == ['2', '2', '0', '0', '0']


def test_info_query_and_tasks(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('info', 'pipeline.py', '-Q', 'n == 1', '--tasks')
    out = capsys.readouterr().out
    assert 'generate[n=1]' in out and 'generate[n=2]' not in out
    assert out.count('success') >= 2


def test_run_task_by_key_prefix(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('info', 'pipeline.py', '--tasks')
    out = capsys.readouterr().out
    key_prefix = next(
        line.split()[1] for line in out.splitlines() if 'generate[n=1]' in line
    )
    cli('run-task', 'pipeline.py', key_prefix)  # does not raise


def test_custom_executor_via_dotted_path(pipeline_dir, capsys):
    Path('myexec.py').write_text('''
from pathlib import Path
from remake import Executor

class MarkerExecutor(Executor):
    def run_tasks(self, tasks):
        Path('marker.txt').write_text(f'{len(tasks)}')
        for task in tasks:
            self.rmk.run_task(task)
''')
    cli('run', 'pipeline.py', '-E', 'myexec:MarkerExecutor')
    assert (pipeline_dir / 'marker.txt').read_text() == '4'
    assert (pipeline_dir / 'data/out_1.txt').exists()


def test_unknown_executor_errors(pipeline_dir):
    from remake import RemakeError

    with pytest.raises(RemakeError, match='Unknown executor'):
        cli('run', 'pipeline.py', '-E', 'bogus')


def test_info_show_failures_prints_traceback(pipeline_dir, capsys):
    Path('failing.py').write_text('''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/f_{n}.txt'}, matrix={'n': [1, 2]})
def sometimes_fails(outputs, n):
    if n == 2:
        raise ValueError('boom from n=2')
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
''')
    # Failures are recorded and the run continues, but the exit code says so.
    assert cli('run', 'failing.py') == 1
    capsys.readouterr()
    cli('info', 'failing.py', '--show-failures')
    out = capsys.readouterr().out
    assert 'sometimes_fails[n=2]' in out and '(failed at' in out
    assert 'Traceback (most recent call last)' in out
    assert 'ValueError: boom from n=2' in out


# A pipeline where many tasks fail the same way, with the differing kwarg
# embedded in the message -- the case the signature-based dedup must collapse.
SAME_BUG = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/{i}.txt'}, matrix={'i': [0, 1, 2, 3]})
def boom(outputs, i):
    if i % 2 == 0:
        raise ValueError(f'bad input for i={i}')
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
'''


def test_info_failures_grouped_by_signature(pipeline_dir, capsys):
    Path('bug.py').write_text(SAME_BUG)
    assert cli('run', 'bug.py') == 1
    capsys.readouterr()

    # Default -F collapses the two i=0/i=2 failures (distinct messages, same
    # traceback) into one group with a count, not two tracebacks.
    cli('info', 'bug.py', '-F')
    out = capsys.readouterr().out
    assert out.count('Traceback (most recent call last)') == 1
    assert '×2' in out and '+ 1 more' in out
    assert 'ValueError' in out and 'boom[i=0]' in out and 'boom[i=2]' in out

    # --all-failures restores the exhaustive per-task dump (two tracebacks).
    capsys.readouterr()
    cli('info', 'bug.py', '--all-failures')
    out = capsys.readouterr().out
    assert out.count('Traceback (most recent call last)') == 2

    # JSON grouped: one group, count 2, both members listed.
    capsys.readouterr()
    cli('info', 'bug.py', '-F', '--json')
    failures = json.loads(capsys.readouterr().out)['failures']
    assert len(failures) == 1
    assert failures[0]['count'] == 2 and len(failures[0]['members']) == 2


def test_info_reasons_tally(pipeline_dir, capsys):
    Path('bug.py').write_text(SAME_BUG)
    cli('run', 'bug.py')  # i=0,2 fail; i=1,3 succeed
    capsys.readouterr()
    cli('info', 'bug.py', '--reasons')
    out = capsys.readouterr().out
    # The two failed tasks would rerun, categorised; successes don't appear.
    assert 'boom: 2 last-run-failed' in out


def test_debug_exception_propagates_task_failure(pipeline_dir, capsys, monkeypatch):
    Path('boom.py').write_text('''
from remake import Remake, rule

@rule(outputs={'o': 'data/{n}.txt'}, matrix={'n': [1]})
def boom(outputs, n):
    raise ValueError('boom')

rmk = Remake()
rmk.rules_from_current_module()
''')
    # Without -X the failure is recorded and the run returns 1.
    assert cli('run', 'boom.py') == 1
    # With -X it propagates (at the interpreter top level the excepthook
    # then drops into pdb/ipdb). Guard the hook remake_cmd installs so it
    # can't fire on unrelated later failures.
    import sys

    monkeypatch.setattr(sys, 'excepthook', sys.excepthook)
    with pytest.raises(ValueError, match='boom'):
        cli('run', 'boom.py', '--force', '-X')


def test_log_file_written(pipeline_dir):
    cli('run', 'pipeline.py')
    log = (pipeline_dir / '.remake/remake.log').read_text()
    assert 'argv' in log and 'pipeline.py' in log


def test_run_task_writes_per_task_log(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('info', 'pipeline.py', '--tasks')
    out = capsys.readouterr().out
    key_prefix = next(
        line.split()[1] for line in out.splitlines() if 'generate[n=1]' in line
    )
    cli('run-task', 'pipeline.py', key_prefix)
    logs = list((pipeline_dir / '.remake/tasks/log/generate').rglob('*.log'))
    assert len(logs) == 1
    assert 'generate[n=1]' in logs[0].read_text()
    # Sharded layout: <rule>/<key[:2]>/<key[2:]>.log
    assert logs[0].parent.name == key_prefix[:2]


def test_ignore_code_changes_flag(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    Path('pipeline.py').write_text(PIPELINE.replace('str(n)', 'str(n * 2)'))
    capsys.readouterr()
    cli('run', 'pipeline.py', '--dry-run')
    assert '4 task(s) would run' in capsys.readouterr().out  # code changed
    cli('run', 'pipeline.py', '--dry-run', '-I')
    assert '0 task(s) would run' in capsys.readouterr().out  # changes ignored


def test_set_state_pending_forces_forget(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('set-state', 'pipeline.py', '-Q', 'rule == "generate" and n == 1', '--pending')
    out = capsys.readouterr().out
    assert 'generate[n=1] -> pending' in out and '1 task(s) set to pending' in out
    # Record gone, but the output still exists: default fallback mode
    # re-adopts it, so nothing reruns.
    cli('run', 'pipeline.py', '--dry-run')
    assert '0 task(s) would run' in capsys.readouterr().out
    # With the output gone too, the forgotten task reruns; element-wise
    # propagation brings process[n=1].
    (pipeline_dir / 'data/raw_1.txt').unlink()
    cli('run', 'pipeline.py', '--dry-run')
    out = capsys.readouterr().out
    assert 'generate[n=1]' in out and '2 task(s) would run' in out


def test_set_state_success_adopts_outputs(pipeline_dir, capsys):
    import json
    import shutil

    cli('run', 'pipeline.py')
    shutil.rmtree('.remake')  # migration scenario: outputs exist, fresh DB
    (pipeline_dir / 'data/out_2.txt').unlink()
    capsys.readouterr()

    cli('set-state', 'pipeline.py', '-Q', 'True', '--success', '--check-outputs', '-n')
    assert 'would be set' in capsys.readouterr().out  # dry run changes nothing

    cli('set-state', 'pipeline.py', '-Q', 'True', '--success', '--check-outputs')
    out = capsys.readouterr().out
    assert '3 task(s) set to success (1 skipped: outputs missing/incomplete)' in out

    cli('info', 'pipeline.py', '--json')
    by_rule = {r['rule']: r for r in json.loads(capsys.readouterr().out)['rules']}
    assert by_rule['generate']['success'] == 2
    assert by_rule['process'] == {
        'rule': 'process', 'deferred': False, 'tasks': 2,
        'success': 1, 'failed': 0, 'pending': 1, 'to_run': 1,
    }


def test_set_state_requires_exactly_one_state(pipeline_dir):
    from remake import RemakeError

    with pytest.raises(RemakeError, match='exactly one'):
        cli('set-state', 'pipeline.py', '-Q', 'True')


def test_info_json(pipeline_dir, capsys):
    import json

    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('info', 'pipeline.py', '--json', '--tasks')
    data = json.loads(capsys.readouterr().out)
    by_rule = {r['rule']: r for r in data['rules']}
    assert by_rule['generate']['success'] == 2 and by_rule['generate']['to_run'] == 0
    assert len(data['tasks']) == 4
    assert all(t['status'] == 'success' and len(t['key']) == 40 for t in data['tasks'])


def test_lint_clean_pipeline(pipeline_dir, capsys):
    assert cli('lint', 'pipeline.py') == 0
    assert 'all inputs wired' in capsys.readouterr().out


def test_lint_flags_near_miss_and_missing_dependency(pipeline_dir, capsys):
    Path('miswired.py').write_text('''
from pathlib import Path
from remake import Remake, rule

@rule(inputs={'src': 'data/src_{n}.txt'},
      outputs={'o': 'data/gen_{n}.txt'}, matrix={'n': [1, 2]})
def gen(inputs, outputs, n):
    pass

@rule(inputs={'i': 'data/genn_{n}.txt'},  # typo: gen makes gen_{n}.txt
      outputs={'o': 'data/proc_{n}.txt'}, matrix=gen.matrix, depends_on=[gen])
def proc(inputs, outputs, n):
    pass

@rule(inputs={'i': 'data/proc_{n}.txt'},  # consumes proc, no depends_on
      outputs={'o': 'data/agg_{n}.txt'}, matrix=gen.matrix)
def agg(inputs, outputs, n):
    pass

rmk = Remake()
rmk.rules_from_current_module()
''')
    assert cli('lint', 'miswired.py') == 1
    out = capsys.readouterr().out
    assert "NEAR MISS           proc: input 'data/genn_1.txt'" in out
    assert "gen produces 'data/gen_1.txt'" in out and '(2 task(s))' in out
    assert 'MISSING DEPENDENCY  agg' in out and 'produced by proc' in out
    assert 'external            gen: 2 input(s)' in out  # not a problem, exit 1 is from above

    import json

    cli('lint', 'miswired.py', '--json')
    rows = json.loads(capsys.readouterr().out)
    assert {r['kind'] for r in rows} == {'near_miss', 'missing_dependency', 'external'}


def test_ls_tasks(pipeline_dir, capsys):
    import json

    cli('ls-tasks', 'pipeline.py')
    out = capsys.readouterr().out
    assert len(out.splitlines()) == 4
    assert 'generate[n=1]' in out and 'process[n=2]' in out

    cli('ls-tasks', 'pipeline.py', '-Q', 'rule == "generate" and n == 2')
    assert capsys.readouterr().out.splitlines() == [
        line for line in out.splitlines() if 'generate[n=2]' in line
    ]

    cli('ls-tasks', 'pipeline.py', '--json', '-Q', 'rule == "process"')
    rows = json.loads(capsys.readouterr().out)
    assert [r['kwargs'] for r in rows] == [{'n': 1}, {'n': 2}]
    assert all(len(r['key']) == 40 and r['rule'] == 'process' for r in rows)


def test_task_info_text_and_json(pipeline_dir, capsys):
    import json

    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('task-info', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')
    out = capsys.readouterr().out
    assert 'generate[n=1]' in out
    assert 'status:   success at ' in out
    assert '[complete]' in out
    assert '.remake/tasks/log/generate/' in out

    cli('task-info', 'pipeline.py', '--json', '-Q', 'rule == "generate" and n == 1')
    data = json.loads(capsys.readouterr().out)
    assert data['status'] == 'success' and data['kwargs'] == {'n': 1}
    assert all(o['complete'] for o in data['outputs'].values())
    assert data['slurm'] == {'jobids': None, 'array_index': None}  # never submitted


def test_task_select_ambiguous_query_errors(pipeline_dir):
    from remake import RemakeError

    cli('run', 'pipeline.py')
    # n == 1 matches one task in each of the two rules sharing the matrix.
    with pytest.raises(RemakeError, match='2 tasks match'):
        cli('task-info', 'pipeline.py', '-Q', 'n == 1')


def test_task_log_path_and_content(pipeline_dir, capsys):
    from remake import RemakeError

    cli('run', 'pipeline.py')  # singleproc run: no per-task logs yet
    capsys.readouterr()
    cli('task-log', 'pipeline.py', '--path', '-Q', 'rule == "generate" and n == 1')
    path = Path(capsys.readouterr().out.strip())
    with pytest.raises(RemakeError, match='No log'):
        cli('task-log', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')

    key = path.parent.name + path.stem  # <k:2>/<k2:>.log
    cli('run-task', 'pipeline.py', key)
    capsys.readouterr()
    cli('task-log', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')
    assert 'generate[n=1]' in capsys.readouterr().out


def test_why_never_run_then_up_to_date(pipeline_dir, capsys):
    cli('why', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out and 'never run' in out

    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('why', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')
    out = capsys.readouterr().out
    assert 'will run: no' in out and 'up to date' in out


def test_why_code_change_and_upstream_propagation(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    Path('pipeline.py').write_text(PIPELINE.replace('str(n)', 'str(n * 2)'))
    capsys.readouterr()
    cli('why', 'pipeline.py', '-Q', 'rule == "generate" and n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out and 'run code changed' in out
    assert '-' in out and '+' in out  # unified diff

    cli('why', 'pipeline.py', '-Q', 'rule == "process" and n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out
    assert 'upstream' in out and 'element-wise' in out
    assert 'generate[n=1]' in out and 'generate[n=2]' not in out


def test_why_multiple_matches_and_runnable_default(pipeline_dir, capsys):
    # -Q matching >1 task explains each (no longer an error), with a summary.
    cli('why', 'pipeline.py', '-Q', 'rule == "generate"')
    out = capsys.readouterr().out
    assert 'generate[n=1]' in out and 'generate[n=2]' in out
    assert out.count('will run: yes') == 2
    assert '2 task(s): 2 would run, 0 up to date' in out

    # Bare `why` explains the runnable set; after a full run, nothing runs.
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('why', 'pipeline.py')
    assert 'nothing would run: all tasks are up to date' in capsys.readouterr().out


def test_run_query_force(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    (pipeline_dir / 'data/out_1.txt').unlink()
    cli('run', 'pipeline.py', '--force', '-Q', 'n == 1')
    assert (pipeline_dir / 'data/out_1.txt').read_text() == '11'
    assert (pipeline_dir / 'data/out_2.txt').exists()
