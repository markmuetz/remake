"""Multiproc executor — spawned workers, sidecar results, per-rule barriers."""
import json
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd

PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'raw': 'data/raw_{n}.txt'}, matrix={'n': [1, 2, 3]})
def generate(outputs, n):
    Path(outputs['raw']).write_text(str(n))

@rule(inputs=generate.outputs, outputs={'out': 'data/out_{n}.txt'},
      matrix=generate.matrix, depends_on=[generate])
def process(inputs, outputs, n):
    Path(outputs['out']).write_text(Path(inputs['raw']).read_text() * 2)

def agg_inputs():
    return {str(n): f'data/out_{n}.txt' for n in [1, 2, 3]}

@rule(inputs=agg_inputs, outputs={'o': 'data/agg.txt'}, depends_on=[process])
def agg(inputs, outputs):
    Path(outputs['o']).write_text(','.join(Path(p).read_text() for p in inputs.values()))

rmk = Remake()
rmk.rules_from_current_module()
'''


def cli(*args):
    return remake_cmd(['remake', *args])


@pytest.fixture
def pipeline_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text(PIPELINE)
    return tmp_path


def test_multiproc_end_to_end(pipeline_dir, capsys):
    # Per-rule barriers make the chain correct: process reads generate's
    # outputs, agg fans in across all of process.
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-j', '2') == 0
    assert Path('data/agg.txt').read_text() == '11,22,33'

    capsys.readouterr()
    cli('info', 'pipeline.py', '--json')
    data = json.loads(capsys.readouterr().out)
    assert all(r['success'] == r['tasks'] and r['to_run'] == 0 for r in data['rules'])
    # Workers' sidecars were ingested, and they wrote per-task logs.
    assert not list(Path('.remake/tasks/results').rglob('*.json'))
    assert len(list(Path('.remake/tasks/log').rglob('*.log'))) == 7


def test_multiproc_failure_exit_code_and_traceback(pipeline_dir, capsys):
    Path('failing.py').write_text('''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/f_{n}.txt'}, matrix={'n': [1, 2]})
def f(outputs, n):
    if n == 2:
        raise ValueError('boom from n=2')
    Path(outputs['o']).write_text('ok')

@rule(inputs=f.outputs, outputs={'o': 'data/g_{n}.txt'},
      matrix=f.matrix, depends_on=[f])
def g(inputs, outputs, n):
    Path(outputs['o']).write_text(Path(inputs['o']).read_text())

rmk = Remake()
rmk.rules_from_current_module()
''')
    assert cli('run', 'failing.py', '-E', 'multiproc', '-j', '2') == 1
    assert Path('data/f_1.txt').exists()  # independent task still ran
    # Downstream of the failure: g[n=2] skipped at the rule barrier,
    # g[n=1] (untainted element) ran.
    assert Path('data/g_1.txt').exists()
    assert not Path('data/g_2.txt').exists()

    capsys.readouterr()
    cli('info', 'failing.py', '-F', '--json')
    data = json.loads(capsys.readouterr().out)
    by_rule = {r['rule']: r for r in data['rules']}
    assert by_rule['g'] == {
        'rule': 'g', 'deferred': False, 'tasks': 2,
        'success': 1, 'failed': 0, 'pending': 1, 'to_run': 1,
    }
    assert 'boom from n=2' in data['failures'][0]['exception']


def test_multiproc_needs_remakefile():
    from remake import MultiprocExecutor, Remake, RemakeError

    with pytest.raises(RemakeError, match='remakefile'):
        MultiprocExecutor(Remake())
