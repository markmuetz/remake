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
    # future.result() and ended the whole run (sys.exit(0) even exited 0)
    # with the remaining tasks never run.
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text('''
import sys
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'x_{n}.txt'}, matrix={'n': [1, 2, 3, 4]})
def exits(outputs, n):
    if n == 2:
        sys.exit(3)
    Path(outputs['o']).write_text('ok')

rmk = Remake()
rmk.rules_from_current_module()
''')
    assert cli('run', 'pipeline.py', '-E', 'multiproc', '-j', '2') == 1
    assert sorted(p.name for p in Path('.').glob('x_*.txt')) == ['x_1.txt', 'x_3.txt', 'x_4.txt']


SLOW = '''
import os, time
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'out/{n}.txt'}, matrix={'n': list(range(6))})
def slow(outputs, n):
    Path('pids').mkdir(exist_ok=True)
    Path(f'pids/{n}').write_text(str(os.getpid()))
    time.sleep(3)
    Path(outputs['o']).write_text('done')

rmk = Remake()
rmk.rules_from_current_module()
'''


@pytest.mark.parametrize('signame, code', [('SIGINT', 130), ('SIGTERM', 143)])
def test_multiproc_interrupt_stops_queue_and_workers(tmp_path, signame, code):
    # Review 2026-09-24 M10: Ctrl-C let every queued task run to completion,
    # and SIGTERM killed the parent but left its workers running (starting
    # new tasks). Signal the parent only, as `kill` would.
    import os
    import signal
    import subprocess
    import sys
    import time

    (tmp_path / 'p.py').write_text(SLOW)
    proc = subprocess.Popen(
        [sys.executable, '-c', 'import sys; from remake.remake_cmd import remake_cmd; '
                               'sys.exit(remake_cmd())', 'run', 'p.py', '-E', 'multiproc',
         '-j', '2'],
        cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    pids = tmp_path / 'pids'
    deadline = time.monotonic() + 30
    while not (pids.is_dir() and len(list(pids.iterdir())) >= 2):
        assert time.monotonic() < deadline, 'workers never started'
        time.sleep(0.05)
    sent = time.monotonic()
    proc.send_signal(getattr(signal, signame))
    _, err = proc.communicate(timeout=30)
    assert proc.returncode == code, err.decode()
    assert time.monotonic() - sent < 2.5  # didn't wait for the queue to drain
    assert 'interrupted' in err.decode() and 'Traceback' not in err.decode()
    assert len(list(pids.iterdir())) == 2  # queued tasks never started
    assert not list((tmp_path / 'out').glob('*.txt')) if (tmp_path / 'out').exists() else True
    time.sleep(0.5)
    for pid_file in pids.iterdir():  # no worker outlives the parent
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid_file.read_text()), 0)
    assert not (tmp_path / '.remake' / 'run.lock').exists()


CRASHER = '''
import os, signal, time
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'a/{n}.txt'}, matrix={'n': list(range(6))})
def a(outputs, n):
    with open('executions.log', 'a') as f:
        f.write(f'{n}\\n')
    time.sleep(0.3)
    if n == 3:
        os.kill(os.getpid(), signal.SIGKILL)  # what the OOM killer does
    Path(outputs['o']).write_text(str(n))

@rule(inputs=a.outputs, outputs={'o': 'b/{n}.txt'}, matrix=a.matrix, depends_on=[a])
def b(inputs, outputs, n):
    Path(outputs['o']).write_text(Path(inputs['o']).read_text())

rmk = Remake()
rmk.rules_from_current_module()
'''


def _check_crash_outcome():
    import sqlite3
    from contextlib import closing

    assert sorted(p.name for p in Path('a').glob('*.txt')) == [f'{n}.txt' for n in (0, 1, 2, 4, 5)]
    with closing(sqlite3.connect('.remake/remake.db')) as conn:
        rows = conn.execute(
            'SELECT r.name, t.last_run_status, t.exception FROM task t '
            'JOIN rule r ON r.id = t.rule_id').fetchall()
    failed = [(name, exc) for name, status, exc in rows if status == 2]
    assert len(failed) == 1 and failed[0][0] == 'a'
    assert 'worker process running this task died' in failed[0][1]
    assert not list(Path('.remake/tasks/running').glob('*'))


def test_multiproc_survives_a_worker_crash(tmp_path, monkeypatch):
    # Review 2026-09-24 H7: a worker killed mid-task (OOM, segfault) raised
    # BrokenProcessPool out of the executor — the run aborted with a
    # traceback, the crasher was never recorded, the rest never ran.
    monkeypatch.chdir(tmp_path)
    Path('p.py').write_text(CRASHER)
    assert cli('run', 'p.py', '-E', 'multiproc', '-j', '3') == 1
    _check_crash_outcome()
    # Downstream of the survivors ran; downstream of the crasher skipped.
    assert sorted(p.name for p in Path('b').glob('*.txt')) == [f'{n}.txt' for n in (0, 1, 2, 4, 5)]
