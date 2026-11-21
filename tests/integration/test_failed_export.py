"""`remake info --failed` and `remake export-task` (field notes 6 and 4)."""
import json
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd

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

FAILING = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/f_{n}.txt'}, matrix={'n': [1, 2]},
      uses={'scale': 3, 'obj': object()})
def sometimes_fails(outputs, n):
    if n == 2:
        raise ValueError('boom from n=2')
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
'''


@pytest.fixture
def pipeline_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text(PIPELINE)
    Path('failing.py').write_text(FAILING)
    return tmp_path


def cli(*args):
    return remake_cmd(['remake', *args])


def test_info_failed_one_line_per_task(pipeline_dir, capsys):
    assert cli('run', 'failing.py') == 1
    capsys.readouterr()
    cli('info', 'failing.py', '--failed')
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    assert lines[0].startswith('sometimes_fails  ')
    assert 'ValueError: boom from n=2' in lines[0]
    assert '.remake/tasks/log/sometimes_fails/' in lines[0]

    cli('info', 'failing.py', '--failed', '--json')
    (row,) = json.loads(capsys.readouterr().out)
    assert row['rule'] == 'sometimes_fails' and len(row['key']) == 40
    assert row['error'] == 'ValueError: boom from n=2'


def test_info_failed_nothing_failed_prints_nothing(pipeline_dir, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('info', 'pipeline.py', '--failed')
    assert capsys.readouterr().out == ''


def test_export_task(pipeline_dir, capsys):
    cli('export-task', 'failing.py', '-Q', 'n == 2')
    spec = json.loads(capsys.readouterr().out)
    assert spec['rule'] == 'sometimes_fails'
    assert spec['kwargs'] == {'n': 2}
    assert spec['inputs'] == {}
    assert spec['outputs'] == {'o': 'data/f_2.txt'}
    assert spec['function']['qualname'] == 'sometimes_fails'
    assert spec['function']['module']
    assert Path(spec['remakefile']) == Path('failing.py').resolve()
    assert spec['uses']['scale'] == 3
    assert spec['uses']['obj'].startswith('<object object')


def test_export_task_to_file_with_inputs(pipeline_dir):
    cli('export-task', 'pipeline.py', '-Q', 'rule == "process" and n == 1',
        '-o', 'spec.json')
    spec = json.loads(Path('spec.json').read_text())
    assert spec['inputs'] == {'raw': 'data/raw_1.txt'}
    assert spec['outputs'] == {'out': 'data/out_1.txt'}


def test_export_task_needs_key_or_query(pipeline_dir, capsys):
    assert cli('export-task', 'pipeline.py') == 2
    assert 'error:' in capsys.readouterr().err
