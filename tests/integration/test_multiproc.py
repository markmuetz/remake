"""Multiproc executor — spawned workers, sidecar results, per-rule barriers."""
from contextlib import closing
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


PROPAGATION = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'a_{n}.txt'}, matrix={'n': [1]})
def a(outputs, n):
    Path(outputs['o']).write_text(str(n * 2))

@rule(inputs=a.outputs, outputs={'o': 'b_{n}.txt'}, matrix=a.matrix, depends_on=[a])
def b(inputs, outputs, n):
    Path(outputs['o']).write_text(str(int(Path(inputs['o']).read_text()) + 1))

rmk = Remake()
rmk.rules_from_current_module()
'''


def test_multiproc_records_run_seq_for_durable_propagation(tmp_path, monkeypatch):
    # Review 2026-09-24 H1: multiproc workers recorded run_seq = NULL, which
    # silently disabled bug 01's durable propagation. Scenario 2 of bug 01
    # under -E multiproc: edit A, rerun only A, then a plain run must rerun B.
    import sqlite3

    monkeypatch.chdir(tmp_path)
    # Same-size edits within one second can replay a stale .pyc (review L11):
    # stop both this process and the spawned workers (env var) writing one.
    monkeypatch.setattr('sys.dont_write_bytecode', True)
    monkeypatch.setenv('PYTHONDONTWRITEBYTECODE', '1')
    Path('pipeline.py').write_text(PROPAGATION)
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-j', '2') == 0
    with closing(sqlite3.connect('.remake/remake.db')) as conn, conn:
        seqs = [r[0] for r in conn.execute('SELECT run_seq FROM task')]
    assert seqs and None not in seqs

    Path('pipeline.py').write_text(PROPAGATION.replace('n * 2', 'n * 3'))
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-Q', "rule == 'a'") == 0
    assert Path('a_1.txt').read_text() == '3'
    assert Path('b_1.txt').read_text() == '3'
    assert cli('run', 'pipeline.py', '-E', 'multiproc') == 0
    assert Path('b_1.txt').read_text() == '4'


def test_multiproc_sys_exit_in_task_is_a_failure(tmp_path, monkeypatch):
    # Review 2026-09-24 H6: a worker's SystemExit came back through
    # future.result() and ended the whole run silently — exit 0 for
    # sys.exit(0) — with the remaining tasks never run.
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text('''
import sys
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'x_{n}.txt'}, matrix={'n': [1, 2, 3, 4]})
def exits(outputs, n):
    if n == 2:
        sys.exit(0)
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
''')
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-j', '2') == 1
    assert sorted(p.name for p in Path('.').glob('x_*.txt')) == ['x_1.txt', 'x_3.txt', 'x_4.txt']
