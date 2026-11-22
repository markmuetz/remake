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
    # Rebuild run in its 1.0 shape by copying: ALTER TABLE DROP COLUMN
    # fails on some system SQLite builds (CI's python3.14, 2026-11-22) when
    # the stored CREATE text has comments between columns.
    conn.execute('CREATE TABLE run_10 (run_seq INTEGER NOT NULL PRIMARY KEY, '
                 'started TIMESTAMP NOT NULL, remakefile TEXT, command TEXT, config TEXT)')
    conn.execute('INSERT INTO run_10 SELECT run_seq, started, remakefile, command, config FROM run')
    conn.execute('DROP TABLE run')
    conn.execute('ALTER TABLE run_10 RENAME TO run')
    assert len(_RUN_COLUMNS_1_1) == 19  # every 1.1 column is re-added by the upgrade
    conn.execute(f'PRAGMA user_version = {SCHEMA_MAJOR * 1000}')
    conn.commit()
    conn.close()

    db = Sqlite3Backend(path)
    cols = {r[1] for r in db.conn.execute('PRAGMA table_info(run)')}
    assert {c for c, _ in _RUN_COLUMNS_1_1} <= cols
    runnable, deferred = Remake(rules=[y], metadata=db).plan()
    assert runnable == [] and deferred == []
    assert db.conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_MAJOR * 1000 + 1


# --- review 2026-11-22 (Fable) of the history commit ---

SKIPPING = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'a_{i}.txt'}, matrix={'i': [1, 2]})
def a(outputs, i):
    raise RuntimeError('a fails')

@rule(inputs={'i': 'a_1.txt'}, outputs={'o': 'b.txt'}, depends_on=[a])
def b(inputs, outputs):
    Path(outputs['o']).write_text('b')

rmk = Remake()
rmk.rules_from_current_module()
'''


def test_counts_separate_skipped_from_run(project):
    Path('pipeline.py').write_text(SKIPPING)
    assert cli('run', 'pipeline.py') == 1
    (run,) = _db().get_runs()
    assert (run['n_run'], run['n_succeeded'], run['n_failed'], run['n_skipped']) == (2, 0, 2, 1)
    assert run['finished'] is not None


def test_error_signature_does_not_leak_a_multiline_message():
    from remake.metadata.sqlite3_backend import error_signature

    tb = ('Traceback (most recent call last):\n'
          '  File "/x/pipeline.py", line 3, in f\n'
          '    raise ValueError(msg)\n'
          'ValueError: bad file /home/me\n'
          'row 3 has password hunter2\n')
    assert error_signature(tb) == 'ValueError at pipeline.py:3 in f'
    assert error_signature('') is None


def test_untracked_remakefile_is_recorded_dirty(project):
    Path('other.py').write_text(PIPELINE)
    cli('run', 'other.py')
    (run,) = _db().get_runs()
    assert run['git_dirty'] is True and run['git_diff_hash']


def test_lockfile_outside_a_repo_is_only_looked_for_beside_the_remakefile(tmp_path):
    from remake.provenance import find_lockfile

    (tmp_path / 'uv.lock').write_text('x')
    (tmp_path / 'sub').mkdir()
    assert find_lockfile(tmp_path / 'sub', in_repo=False) == (None, None)
    assert find_lockfile(tmp_path, in_repo=False)[0].name == 'uv.lock'


def _sidecar_db(tmp_path):
    """A backend with one rule and a SidecarWriter-run task, for ingest tests."""
    from remake.metadata.sidecar import SidecarWriter

    @rule(outputs={'o': 'x_{i}.txt'}, matrix={'i': [1]})
    def x(outputs, i):
        Path(outputs['o']).write_text('x')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[x], metadata=db)
    rmk.finalize()
    (task,) = rmk.tasks()
    return db, rmk, x, task, SidecarWriter


def test_a_sidecar_ingested_twice_is_one_history_row(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db, rmk, x, task, SidecarWriter = _sidecar_db(tmp_path)
    rmk.metadata = SidecarWriter(run_seq=5)
    rmk.run_task(task)
    (sidecar,) = Path('.remake/tasks/results').rglob('*.json')
    saved = sidecar.read_text()
    db.ingest_sidecars([x])
    sidecar.write_text(saved)  # a crash before the unlink, or a second ingester
    db.ingest_sidecars([x])
    assert len(db.get_history(task)) == 1


def test_history_cap_applies_to_ingest_outside_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db, rmk, x, task, SidecarWriter = _sidecar_db(tmp_path)
    db.history_keep = 3
    for seq in range(1, 6):
        rmk.metadata = SidecarWriter(run_seq=seq)
        rmk.run_task(task)
        db.ingest_sidecars([x])
    rows = db.get_history(task)
    assert [r['run_seq'] for r in rows] == [5, 4, 3]


def test_late_older_sidecar_does_not_become_the_last_execution(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db, rmk, x, task, SidecarWriter = _sidecar_db(tmp_path)
    for seq in (9, 2):
        rmk.metadata = SidecarWriter(run_seq=seq)
        rmk.run_task(task)
        db.ingest_sidecars([x])
    assert [r['run_seq'] for r in db.get_history(task)] == [9, 2]


def test_history_keep_is_validated(project, capsys):
    cli('run', 'pipeline.py')
    assert cli('history', 'pipeline.py', '--prune', '--keep', '0') == 2
    assert cli('history', 'pipeline.py', '--keep', '3') == 2
    Path('remake.toml').write_text('[history]\nkeep = 0\n')
    assert cli('info', 'pipeline.py') == 2


def test_delete_tasks_removes_their_history(project):
    cli('run', 'pipeline.py')
    db = _db()
    tasks = _load_tasks()
    db.delete_tasks(tasks)
    assert db.conn.execute('SELECT COUNT(*) FROM task_run').fetchone()[0] == 0


def test_failed_output_check_signature_names_the_output():
    from remake.metadata.sqlite3_backend import error_signature

    tb = ('Traceback (most recent call last):\n'
          '  File "/x/remake.py", line 9, in _check_outputs_after_run\n'
          '    raise OutputValidationError(\n'
          'remake.core.exceptions.OutputValidationError: s[]: output check(s) failed:\n'
          '  summary: data/summary.txt is empty\n')
    assert error_signature(tb) == 'OutputValidationError: summary'


def test_set_state_closes_its_run_row(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #13.
    from remake import Remake, Sqlite3Backend, rule

    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'o_{i}.txt'}, matrix={'i': [1, 2]})
    def r13(outputs, i):
        Path(outputs['o']).write_text('x')

    rmk = Remake(rules=[r13], metadata=Sqlite3Backend(tmp_path / 'r.db'))
    rmk.run()
    rmk.set_state("rule == 'r13'", pending=True)
    latest = rmk.history(runs=True)[0]
    assert latest['command'] == 'set-state'
    assert latest['finished'] is not None and latest['n_planned'] == 2
