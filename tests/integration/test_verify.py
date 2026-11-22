"""`remake verify` (0.10 item 2)."""
import json
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd

PIPELINE = '''
from pathlib import Path
from remake import Dir, Ensure, Remake, rule

@rule(outputs={'o': Ensure('a_{i}.txt', min_size=3)}, matrix={'i': [1, 2, 3]})
def a(outputs, i):
    Path(outputs['o']).write_text('aaa')

@rule(inputs={'i': 'a_1.txt'}, outputs={'d': Dir('plots')}, depends_on=[a])
def b(inputs, outputs):
    Path(outputs['d'], 'p.png').write_text('p')

rmk = Remake()
rmk.rules_from_current_module()
'''


def cli(*argv):
    return remake_cmd(['remake', *argv]) or 0


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text(PIPELINE)
    return tmp_path


def _verify_json(capsys, *extra):
    capsys.readouterr()
    code = cli('verify', 'pipeline.py', '--json', *extra)
    return code, json.loads(capsys.readouterr().out)


def test_clean_pipeline_has_nothing_to_report(project, capsys):
    cli('run', 'pipeline.py')
    code, data = _verify_json(capsys)
    assert code == 0 and data['format'] == 1
    assert data['missing'] == [] and data['adoptable'] == []


def test_purged_and_truncated_outputs_are_found_and_marked_pending(project, capsys):
    cli('run', 'pipeline.py')
    Path('a_2.txt').unlink()
    Path('a_3.txt').write_text('x')  # fails Ensure(min_size=3)
    code, data = _verify_json(capsys, '--outputs')
    assert code == 1
    assert sorted(r['task'].split()[-1] for r in data['missing']) == ['a[i=2]', 'a[i=3]']
    # Read-only: still nothing planned.
    capsys.readouterr()
    cli('run', 'pipeline.py', '-n')
    assert '0 task(s) would run' in capsys.readouterr().out
    code, data = _verify_json(capsys, '--outputs', '--apply')
    assert code == 0 and data['applied']
    capsys.readouterr()
    cli('run', 'pipeline.py', '-n')
    out = capsys.readouterr().out
    assert '2 task(s) would run' in out  # a[2], a[3]; b reads only a_1


def test_existing_output_tree_is_adopted(project, capsys):
    for i in (1, 2, 3):
        Path(f'a_{i}.txt').write_text('aaa')
    Path('plots').mkdir()
    Path('plots/p.png').write_text('p')  # a Dir without its manifest
    code, data = _verify_json(capsys, '--adopt')
    assert code == 1 and len(data['adoptable']) == 4
    assert not Path('plots/.remake-manifest.json').exists()  # read-only
    code, data = _verify_json(capsys, '--adopt', '--apply')
    assert code == 0
    assert Path('plots/.remake-manifest.json').exists()
    capsys.readouterr()
    cli('run', 'pipeline.py', '-n')
    assert '0 task(s) would run' in capsys.readouterr().out


def test_query_limits_verify(project, capsys):
    cli('run', 'pipeline.py')
    Path('a_2.txt').unlink()
    code, data = _verify_json(capsys, '-Q', 'i == 1')
    assert code == 0 and data['missing'] == []
