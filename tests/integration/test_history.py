"""Provenance capture and run history (0.10 items 1 and 3)."""
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from remake import Remake, Sqlite3Backend, rule
from remake.metadata.sqlite3_backend import SCHEMA_MAJOR, _RUN_COLUMNS_1_1
from remake.remake_cmd import remake_cmd

PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'a_{i}.txt'}, matrix={'i': [1, 2]})
def a(outputs, i):
    Path(outputs['o']).write_text(str(i) * 10)

@rule(inputs={'i': 'a_1.txt'}, outputs={'o': 'b.txt'}, depends_on=[a])
def b(inputs, outputs):
    Path(outputs['o']).write_text(Path(inputs['i']).read_text())

rmk = Remake()
rmk.rules_from_current_module()
'''


def cli(*argv):
    """Run the remake CLI in-process; its exit code."""
    return remake_cmd(['remake', *argv]) or 0


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A git repo with a committed pipeline, cwd inside it."""
    monkeypatch.chdir(tmp_path)
    Path('pipeline.py').write_text(PIPELINE)
    git = ['git', '-c', 'user.name=t', '-c', 'user.email=t@example.invalid']
    try:
        subprocess.run(['git', 'init', '-q'], check=True)
        subprocess.run([*git, 'add', 'pipeline.py'], check=True)
        subprocess.run([*git, 'commit', '-qm', 'init'], check=True)
    except (OSError, subprocess.CalledProcessError):
        pytest.skip('git not available')
    return tmp_path


def _db():
    return Sqlite3Backend('.remake/remake.db')


def test_run_records_provenance_and_counts(project):
    assert cli('run', 'pipeline.py') == 0
    db = _db()
    (run,) = db.get_runs()
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                          text=True).stdout.strip()
    assert run['git_commit'] == head and run['git_dirty'] is False
    assert run['remake_version'] and run['python_version']
    assert run['host'] and run['user_hash'] and len(run['user_hash']) == 16
    assert (run['n_planned'], run['n_run'], run['n_succeeded'], run['n_failed'],
            run['n_waves']) == (3, 3, 3, 0, 1)
    env = db.get_environment(db.conn.execute(
        'SELECT env_id FROM run').fetchone()[0])
    assert any(p.startswith('remake==') for p in env['packages'])


def test_dirty_tree_records_a_diff_hash(project):
    Path('pipeline.py').write_text(PIPELINE + '\n# edit\n')
    cli('run', 'pipeline.py')
    (run,) = _db().get_runs()
    assert run['git_dirty'] is True and len(run['git_diff_hash']) == 64


def test_history_keeps_every_execution_with_its_reason(project, capsys):
    cli('run', 'pipeline.py')
    cli('run', 'pipeline.py', '-f', '-Q', "rule == 'b'")
    db = _db()
    rmk_b = [t for t in _load_tasks() if t.rule.name == 'b'][0]
    rows = db.get_history(rmk_b)
    assert [r['reason'] for r in rows] == ['forced', 'never-run']
    assert all(r['host'] for r in rows)
    assert rows[0]['output_bytes'] == 10
    capsys.readouterr()
    assert cli('history', 'pipeline.py', '-Q', "rule == 'b'", '--json') == 0
    data = json.loads(capsys.readouterr().out)
    assert data['format'] == 1 and len(data['executions']) == 2
    assert cli('history', 'pipeline.py', '--runs') == 0
    out = capsys.readouterr().out
    assert len(out.strip().splitlines()) == 2


def _load_tasks():
    from remake import load_remake

    rmk = load_remake('pipeline.py')
    return list(rmk.tasks())


def test_failed_execution_records_an_error_signature(project):
    Path('pipeline.py').write_text(PIPELINE.replace(
        "Path(outputs['o']).write_text(Path(inputs['i']).read_text())",
        "raise ValueError('nope')"))
    assert cli('run', 'pipeline.py') == 1
    (b,) = [t for t in _load_tasks() if t.rule.name == 'b']
    (row,) = _db().get_history(b)
    assert row['error'].startswith('ValueError at pipeline.py:')
    (run,) = _db().get_runs()
    assert run['n_failed'] == 1


def test_history_is_capped_per_task(project, capsys):
    Path('remake.toml').write_text('[history]\nkeep = 2\n')
    for _ in range(4):
        cli('run', 'pipeline.py', '-f')
    (b,) = [t for t in _load_tasks() if t.rule.name == 'b']
    assert len(_db().get_history(b)) == 2
    capsys.readouterr()
    assert cli('history', 'pipeline.py', '--prune', '--keep', '1') == 0
    assert 'pruned 3 history row(s)' in capsys.readouterr().out
    assert len(_db().get_history(b)) == 1


def test_task_info_shows_the_last_execution(project, capsys):
    cli('run', 'pipeline.py')
    capsys.readouterr()
    cli('task-info', 'pipeline.py', '-Q', "rule == 'b'")
    out = capsys.readouterr().out
    assert 'last ran: run 1' in out and '(never-run)' in out
    assert 'git:      ' in out and 'env:      Python' in out


def test_set_state_writes_no_history(project):
    cli('set-state', 'pipeline.py', '-Q', 'True', '--success')
    (b,) = [t for t in _load_tasks() if t.rule.name == 'b']
    assert _db().get_history(b) == []


def test_sidecar_execution_in_other_environment_is_flagged(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from remake.metadata.sidecar import SidecarWriter

    @rule(outputs={'o': 'x.txt'})
    def x(outputs):
        Path(outputs['o']).write_text('x')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[x], metadata=db)
    rmk.finalize()
    db.record_run(command='run', provenance={'env_fingerprint': 'planner',
                                             'env_packages': ['a==1']})
    run_seq = db.current_run_seq()
    (task,) = rmk.tasks()
    # What run-array-task does on a compute node with other packages.
    rmk.metadata = SidecarWriter(run_seq=run_seq)
    rmk.planner_env = 'planner'
    task.rerun_reason = 'never-run'
    rmk.run_task(task)
    db.ingest_sidecars([x])
    (row,) = db.get_history(task)
    assert row['reason'] == 'never-run'
    assert row['env_id'] is not None and row['env_id'] != row['run_env_id']


def test_upgrade_from_schema_1_0_adds_history_and_reruns_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'y.txt'})
    def y(outputs):
        Path(outputs['o']).write_text('y')

    path = tmp_path / 'remake.db'
    db = Sqlite3Backend(path)
    assert Remake(rules=[y], metadata=db).run() == 0
    db.close()
    # Turn it back into a 0.9.0 (schema 1.0) DB.
    conn = sqlite3.connect(path)
    conn.execute('DROP TABLE task_run')
    conn.execute('DROP TABLE environment')
    for col, _ in _RUN_COLUMNS_1_1:
        conn.execute(f'ALTER TABLE run DROP COLUMN {col}')
    conn.execute(f'PRAGMA user_version = {SCHEMA_MAJOR * 1000}')
    conn.commit()
    conn.close()

    db = Sqlite3Backend(path)
    cols = {r[1] for r in db.conn.execute('PRAGMA table_info(run)')}
    assert {c for c, _ in _RUN_COLUMNS_1_1} <= cols
    runnable, deferred = Remake(rules=[y], metadata=db).plan()
    assert runnable == [] and deferred == []
    assert db.conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_MAJOR * 1000 + 1
