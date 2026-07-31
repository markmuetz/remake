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
    assert all(r['up_to_date'] == r['tasks'] and r['to_run'] == 0 for r in data['rules'])
    # Workers' sidecars were ingested, and they wrote per-task logs.
    assert not list(Path('.remake/tasks/results').rglob('*.json'))
    assert len(list(Path('.remake/tasks/log').rglob('*.log'))) == 7


def test_multiproc_records_resources_via_sidecars(pipeline_dir, capsys):
    # Resources are measured in the worker and reach the DB through the
    # sidecar/ingest path (design_docs/resource_capture.md).
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-j', '2') == 0

    capsys.readouterr()
    cli('info', 'pipeline.py', '--tasks', '--json')
    key = json.loads(capsys.readouterr().out)['tasks'][0]['key']

    cli('task-info', 'pipeline.py', key, '--json')
    resources = json.loads(capsys.readouterr().out)['resources']
    assert resources['wall_s'] is not None
    # Pooled workers run several tasks each, so this is exactly the case
    # getrusage would mis-attribute: it must come from the sampler.
    assert resources['rss_method'] == 'sample'
    assert resources['max_rss_bytes'] > 0


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
        'up_to_date': 1, 'stale': 0, 'failed': 0, 'pending': 1, 'to_run': 1,
    }
    # -F --json now groups failures; the traceback is on the representative.
    assert 'boom from n=2' in data['failures'][0]['example']['exception']


def test_multiproc_needs_remakefile():
    from remake import MultiprocExecutor, Remake, RemakeError

    with pytest.raises(RemakeError, match='remakefile'):
        MultiprocExecutor(Remake())


def test_default_nproc_respects_cpu_affinity(monkeypatch):
    # The default must come from the cpuset mask (sched_getaffinity), not
    # the machine's total cores -- otherwise inside a SLURM allocation on a
    # big shared node multiproc oversubscribes. See _default_nproc.
    import remake.executors.multiproc_executor as mp

    monkeypatch.setattr(mp.os, 'sched_getaffinity', lambda pid: {0, 1, 2, 3}, raising=False)
    monkeypatch.setattr(mp.os, 'cpu_count', lambda: 48)
    assert mp._default_nproc() == 4  # the affinity mask, not 48


def test_default_nproc_falls_back_without_affinity(monkeypatch):
    # Non-Linux: sched_getaffinity may be absent -> fall back to cpu_count.
    import remake.executors.multiproc_executor as mp

    monkeypatch.delattr(mp.os, 'sched_getaffinity', raising=False)
    monkeypatch.setattr(mp.os, 'cpu_count', lambda: 12)
    assert mp._default_nproc() == 12
