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


def test_run_query_force(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    (pipeline_dir / 'data/out_1.txt').unlink()
    cli('run', 'pipeline.py', '--force', '-Q', 'n == 1')
    assert (pipeline_dir / 'data/out_1.txt').read_text() == '11'
    assert (pipeline_dir / 'data/out_2.txt').exists()
