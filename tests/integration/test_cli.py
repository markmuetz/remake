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
    cli('run', 'pipeline.py')
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
    cli('run', 'failing.py')
    capsys.readouterr()
    cli('info', 'failing.py', '--show-failures')
    out = capsys.readouterr().out
    assert 'sometimes_fails[n=2]' in out and '(failed at' in out
    assert 'Traceback (most recent call last)' in out
    assert 'ValueError: boom from n=2' in out


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


def test_ls_tasks(pipeline_dir, capsys):
    import json

    cli('ls-tasks', 'pipeline.py')
    out = capsys.readouterr().out
    assert len(out.splitlines()) == 4
    assert 'generate[n=1]' in out and 'process[n=2]' in out

    cli('ls-tasks', 'pipeline.py', '-Q', 'n == 2', '-R', 'generate')
    assert capsys.readouterr().out.splitlines() == [
        line for line in out.splitlines() if 'generate[n=2]' in line
    ]

    cli('ls-tasks', 'pipeline.py', '--json', '-R', 'process')
    rows = json.loads(capsys.readouterr().out)
    assert [r['kwargs'] for r in rows] == [{'n': 1}, {'n': 2}]
    assert all(len(r['key']) == 40 and r['rule'] == 'process' for r in rows)


def test_task_info_text_and_json(pipeline_dir, capsys):
    import json

    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('task-info', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')
    out = capsys.readouterr().out
    assert 'generate[n=1]' in out
    assert 'status:   success at ' in out
    assert '[complete]' in out
    assert '.remake/tasks/log/generate/' in out

    cli('task-info', 'pipeline.py', '--json', '-R', 'generate', '-Q', 'n == 1')
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
    cli('task-log', 'pipeline.py', '--path', '-R', 'generate', '-Q', 'n == 1')
    path = Path(capsys.readouterr().out.strip())
    with pytest.raises(RemakeError, match='No log'):
        cli('task-log', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')

    key = path.parent.name + path.stem  # <k:2>/<k2:>.log
    cli('run-task', 'pipeline.py', key)
    capsys.readouterr()
    cli('task-log', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')
    assert 'generate[n=1]' in capsys.readouterr().out


def test_why_never_run_then_up_to_date(pipeline_dir, capsys):
    cli('why', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out and 'never run' in out

    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('why', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')
    out = capsys.readouterr().out
    assert 'will run: no' in out and 'up to date' in out


def test_why_code_change_and_upstream_propagation(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    Path('pipeline.py').write_text(PIPELINE.replace('str(n)', 'str(n * 2)'))
    capsys.readouterr()
    cli('why', 'pipeline.py', '-R', 'generate', '-Q', 'n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out and 'run code changed' in out
    assert '-' in out and '+' in out  # unified diff

    cli('why', 'pipeline.py', '-R', 'process', '-Q', 'n == 1')
    out = capsys.readouterr().out
    assert 'will run: yes' in out
    assert 'upstream' in out and 'element-wise' in out
    assert 'generate[n=1]' in out and 'generate[n=2]' not in out


def test_run_query_force(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    (pipeline_dir / 'data/out_1.txt').unlink()
    cli('run', 'pipeline.py', '--force', '-Q', 'n == 1')
    assert (pipeline_dir / 'data/out_1.txt').read_text() == '11'
    assert (pipeline_dir / 'data/out_2.txt').exists()
