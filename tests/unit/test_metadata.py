from contextlib import closing
import sqlite3
from pathlib import Path
from time import perf_counter

import pytest
from loguru import logger

from remake import Remake, Sqlite3Backend, rule
from remake.metadata import TASK_STATUS_SUCCESS


def _capture_warnings(fn):
    """Run fn, returning the WARNING-level loguru messages it emits."""
    msgs = []
    sink = logger.add(msgs.append, level='WARNING', format='{message}')
    try:
        fn()
    finally:
        logger.remove(sink)
    return msgs


def _two_same_named_rules(tmp_path):
    # Two distinct rules that share the name 'process' (the function name) but
    # have genuinely different source bodies.
    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):
        Path(outputs['o']).write_text('one')

    r1 = process

    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):  # noqa: F811 — deliberately same name
        Path(outputs['o']).write_text('two')

    return r1, process


def test_backend_context_manager_closes_connection():
    with Sqlite3Backend(':memory:') as meta:
        assert meta.conn.execute('SELECT 1').fetchone() == (1,)
    with pytest.raises(sqlite3.ProgrammingError):
        meta.conn.execute('SELECT 1')


def test_duplicate_rule_name_across_remakefiles_warns(tmp_path):
    # Two co-located remakefiles defining a different rule under the same name
    # share one .remake/ store and would clobber each other — ensure_rules must
    # warn (the duplicate-rule-name guard).
    meta = Sqlite3Backend(':memory:')
    r1, r2 = _two_same_named_rules(tmp_path)

    meta.ensure_rules([r1], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([r2], remakefile='b.py'))

    text = ' '.join(msgs)
    assert 'process' in text and 'a.py' in text and 'b.py' in text


def test_same_remakefile_edit_does_not_warn(tmp_path):
    # The same name with changed code from the *same* remakefile is an ordinary
    # edit, not a collision — no warning.
    meta = Sqlite3Backend(':memory:')
    r1, r2 = _two_same_named_rules(tmp_path)

    meta.ensure_rules([r1], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([r2], remakefile='a.py'))

    assert not any('defined in both' in m for m in msgs)


# The pre-FK schema: uses_hash/io_hash stored the full normalised strings
# inline on every task row (what _migrate_inline_hashes_to_code_ids upgrades).
OLD_SCHEMA = """
CREATE TABLE code (
    id INTEGER NOT NULL, code TEXT NOT NULL, PRIMARY KEY (id)
);
CREATE TABLE rule (
    id INTEGER NOT NULL, name VARCHAR(200) NOT NULL,
    inputs_code_id INTEGER NOT NULL, outputs_code_id INTEGER NOT NULL,
    run_code_id INTEGER NOT NULL, remakefile TEXT, PRIMARY KEY (id)
);
CREATE TABLE task (
    id INTEGER NOT NULL, key VARCHAR(40) NOT NULL, rule_id INTEGER NOT NULL,
    run_code_id INTEGER, uses_hash TEXT, io_hash TEXT, run_seq INTEGER,
    last_run_timestamp TIMESTAMP, last_run_status INTEGER, exception TEXT,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX task_key_index ON task(key);
CREATE TABLE meta (key TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO meta(key, value) VALUES ('run_seq', 1);
"""


def _migration_pipeline(tmp_path):
    @rule(outputs={'o': str(tmp_path / '{n}.txt')}, matrix={'n': [1, 2]},
          uses={'K': 3})
    def process(outputs, n):
        Path(outputs['o']).write_text(str(n))

    return process


def _write_old_db(dbloc, r, tasks):
    from remake.core.scope import io_hash, uses_hash

    conn = sqlite3.connect(dbloc)
    conn.executescript(OLD_SCHEMA)
    src = r.source
    code_ids = {}
    for part in ('inputs', 'outputs', 'run'):
        cur = conn.execute('INSERT INTO code(code) VALUES (?)', (src[part],))
        code_ids[part] = cur.lastrowid
    cur = conn.execute(
        'INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id, '
        'remakefile) VALUES (?, ?, ?, ?, ?)',
        (r.name, code_ids['inputs'], code_ids['outputs'], code_ids['run'], 'rf.py'))
    rule_id = cur.lastrowid
    # One fully-tracked success, one pre-io_hash record (io_hash NULL) — the
    # NULL must survive as io_code_id NULL (not-tracked, no rerun on upgrade).
    for task, io in [(tasks[0], io_hash(r)), (tasks[1], None)]:
        conn.execute(
            'INSERT INTO task(key, rule_id, run_code_id, uses_hash, io_hash, '
            "run_seq, last_run_timestamp, last_run_status, exception) "
            "VALUES (?, ?, ?, ?, ?, 1, datetime('now'), 1, '')",
            (task.key, rule_id, code_ids['run'], uses_hash(r.uses), io))
    conn.commit()
    conn.close()


def test_old_inline_hash_db_migrates_without_mass_rerun(tmp_path):
    from remake import Remake
    from remake.core.dag import expand_rule

    r = _migration_pipeline(tmp_path)
    tasks = expand_rule(r)
    dbloc = tmp_path / 'remake.db'
    _write_old_db(dbloc, r, tasks)

    meta = Sqlite3Backend(dbloc)
    cols = {row[1] for row in meta.conn.execute('PRAGMA table_info(task)')}
    assert {'uses_code_id', 'io_code_id'} <= cols
    # DROP COLUMN needs SQLite >= 3.35; on older versions the columns remain
    # but are NULLed out.
    if 'uses_hash' in cols:
        assert meta.conn.execute(
            'SELECT count(*) FROM task WHERE uses_hash IS NOT NULL').fetchone()[0] == 0

    records = meta.get_tasks_status(tasks)
    codes = meta.get_codes({rec.uses_code_id for rec in records.values()})
    from remake.core.scope import uses_hash
    assert all(codes[rec.uses_code_id] == uses_hash(r.uses)
               for rec in records.values())
    assert records[tasks[1].key].io_code_id is None  # NULL preserved

    # The real acceptance test: nothing reruns after the migration.
    rmk = Remake(rules=[r], metadata=meta)
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_uses_change_after_migration_reruns(tmp_path):
    from remake import Remake
    from remake.core.dag import expand_rule

    r = _migration_pipeline(tmp_path)
    tasks = expand_rule(r)
    dbloc = tmp_path / 'remake.db'
    _write_old_db(dbloc, r, tasks)

    r.uses = {'K': 4}
    rmk = Remake(rules=[r], metadata=Sqlite3Backend(dbloc))
    runnable, _ = rmk.plan()
    assert len(runnable) == 2  # both tasks: uses= changed


# --- status-query scaling regression (design_docs/logs_analysis §1.1/1.2) ---
#
# The field failure this guards against: get_tasks_status once returned the
# full run source per task (a JOIN on code.code) and task rows stored the
# full normalised uses text inline — so a rule with a large `uses=` made a
# 1465-task status query scan ~213 MB (2.2 s) and a 3341-task DB weigh
# 272 MB. Post-rework, rows carry integer FKs and text is fetched once per
# *distinct* id, so per-task cost must be independent of source size. Wall
# clocks are too noisy for CI, so assert the scaling *ratio*: same task
# count, tiny uses vs a wescon-sized (~150 KB rendered) uses — pre-rework
# the ratio was ~100×, correct behaviour is ~1×.

N_SCALING_TASKS = 2000
SCALING_RATIO_LIMIT = 5.0  # generous headroom over ~1× for CI noise
TIMER_FLOOR = 0.05  # seconds; below this, ratios are timer noise


def _populated_pipeline(tmp_path, uses):
    @rule(outputs={'o': str(tmp_path / '{n}.txt')},
          matrix={'n': list(range(N_SCALING_TASKS))}, uses=uses)
    def process(outputs, n):
        Path(outputs['o']).write_text(str(n))

    rmk = Remake(rules=[process], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    tasks = rmk.tasks()
    rmk.metadata.update_tasks(tasks, TASK_STATUS_SUCCESS)
    return rmk, tasks


def _best_of(fn, n=3):
    best = None
    for _ in range(n):
        start = perf_counter()
        fn()
        elapsed = perf_counter() - start
        if best is None or elapsed < best:
            best = elapsed
    return best


def test_status_query_time_independent_of_uses_size(tmp_path):
    from remake.core.scope import uses_hash

    big_uses = {'table': list(range(30000))}  # repr ~ a wescon-sized blob
    assert len(uses_hash(big_uses)) > 150_000
    rmk_small, tasks_small = _populated_pipeline(tmp_path / 's', {'k': 1})
    rmk_big, tasks_big = _populated_pipeline(tmp_path / 'b', big_uses)

    # Structural canary, timer-free: records carry ids, never text.
    rec = next(iter(rmk_big.metadata.get_tasks_status(tasks_big[:1]).values()))
    assert isinstance(rec.uses_code_id, int) and isinstance(rec.run_code_id, int)

    t_small = _best_of(lambda: rmk_small.metadata.get_tasks_status(tasks_small))
    t_big = _best_of(lambda: rmk_big.metadata.get_tasks_status(tasks_big))
    assert t_big <= max(SCALING_RATIO_LIMIT * t_small, TIMER_FLOOR), (
        f'status query scales with uses size again: '
        f'{t_big:.3f}s (big uses) vs {t_small:.3f}s (small uses) '
        f'for {N_SCALING_TASKS} tasks — is per-task text back in the SELECT?')

    # plan() sits on top of the status query (fetches each distinct code id
    # once, compares once per rule): must show the same independence.
    t_plan_small = _best_of(lambda: rmk_small.plan())
    t_plan_big = _best_of(lambda: rmk_big.plan())
    assert t_plan_big <= max(SCALING_RATIO_LIMIT * t_plan_small, 2 * TIMER_FLOOR), (
        f'plan() scales with uses size again: '
        f'{t_plan_big:.3f}s (big uses) vs {t_plan_small:.3f}s (small uses)')

    # And nothing should have been planned as a rerun (guards against the
    # timing comparison passing while the semantics silently broke).
    assert not rmk_big.plan()[0] and not rmk_small.plan()[0]


def test_uses_manifest_records_raw_source_per_version(tmp_path):
    # ensure_rules writes one uses_manifest row per helper, keyed by the uses
    # version (uses_code_id) — so old versions' raw sources stay resolvable
    # after the rule moves on, and shared helpers intern to one code row.
    def helper(x):
        return x + 1

    @rule(outputs={'o': str(tmp_path / 'o.txt')},
          uses={'helper': helper, 'K': 3})
    def process(outputs):
        Path(outputs['o']).write_text(str(helper(K)))  # noqa: F821 — injected

    meta = Sqlite3Backend(':memory:')
    meta.ensure_rules([process], remakefile='rf.py')
    _, _, uses_code_id, _ = meta.rule_ids['process']

    manifest = meta.get_uses_manifest(uses_code_id)
    assert set(manifest) == {'helper', 'K'}
    raw, kind = manifest['helper']
    assert kind == 'source' and 'return x + 1' in raw
    assert manifest['K'] == ('3', 'value')

    # Write-once: re-ensuring doesn't duplicate rows.
    meta.ensure_rules([process], remakefile='rf.py')
    (n,) = meta.conn.execute(
        'SELECT count(*) FROM uses_manifest WHERE uses_code_id = ?',
        (uses_code_id,)).fetchone()
    assert n == 2


def test_identical_shared_rule_does_not_warn(tmp_path):
    # The same rule object registered from two remakefiles (identical source) is
    # legitimate sharing — provenance moves, but no collision warning.
    meta = Sqlite3Backend(':memory:')

    @rule(outputs={'o': str(tmp_path / 'o.txt')})
    def process(outputs):
        Path(outputs['o']).write_text('shared')

    meta.ensure_rules([process], remakefile='a.py')
    msgs = _capture_warnings(lambda: meta.ensure_rules([process], remakefile='b.py'))

    assert not any('defined in both' in m for m in msgs)


# --- review 2026-09-24 M5/M6: atomic schema create + migrations ---


def test_zero_byte_db_is_initialised_not_bricked(tmp_path):
    # M5: a 0-byte file (first run killed mid-create, disk/quota full) used
    # to count as "existing", so every later open failed with
    # "no such table: task".
    db = tmp_path / 'remake.db'
    db.touch()
    with Sqlite3Backend(db) as meta:
        tables = {r[0] for r in meta.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {'task', 'rule', 'code', 'meta'} <= tables


def test_failed_schema_create_leaves_a_recoverable_db(tmp_path, monkeypatch):
    # M5: schema creation is one transaction — dying half-way leaves no
    # tables at all, and the next open creates the schema from scratch.
    import remake.metadata.sqlite3_backend as backend

    real = backend._schema_statements

    def half():
        stmts = real()
        return stmts[:3] + ['THIS IS NOT SQL']

    db = tmp_path / 'remake.db'
    monkeypatch.setattr(backend, '_schema_statements', half)
    with pytest.raises(sqlite3.OperationalError):
        Sqlite3Backend(db)
    monkeypatch.setattr(backend, '_schema_statements', real)
    with closing(sqlite3.connect(db)) as conn, conn:
        assert not list(conn.execute("SELECT name FROM sqlite_master WHERE type='table'"))
    with Sqlite3Backend(db) as meta:
        assert meta.conn.execute("SELECT value FROM meta WHERE key='run_seq'").fetchone() == (0,)


def _old_db(path):
    """A DB as written before run_seq/meta existed (0.8.0a0-era shape)."""
    with Sqlite3Backend(path):
        pass
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute('ALTER TABLE task DROP COLUMN run_seq')
        conn.execute('DROP TABLE meta')


def test_failed_migration_rolls_back_and_is_redone(tmp_path, monkeypatch):
    # M6: migrations used to autocommit statement by statement, so an
    # interrupted one left a half-migrated DB that was then skipped forever.
    db = tmp_path / 'remake.db'
    _old_db(db)
    real = Sqlite3Backend._add_missing_columns

    def dies_half_way(self):
        self.conn.execute('ALTER TABLE task ADD COLUMN run_seq INTEGER')
        raise KeyboardInterrupt

    monkeypatch.setattr(Sqlite3Backend, '_add_missing_columns', dies_half_way)
    with pytest.raises(KeyboardInterrupt):
        Sqlite3Backend(db)
    with closing(sqlite3.connect(db)) as conn, conn:
        assert 'run_seq' not in {r[1] for r in conn.execute('PRAGMA table_info(task)')}
    monkeypatch.setattr(Sqlite3Backend, '_add_missing_columns', real)
    with Sqlite3Backend(db) as meta:
        assert 'run_seq' in {r[1] for r in meta.conn.execute('PRAGMA table_info(task)')}
        assert meta.conn.execute("SELECT value FROM meta WHERE key='run_seq'").fetchone() == (0,)


def _open(db):
    Sqlite3Backend(db).close()
    return 'ok'


def test_concurrent_first_opens_do_not_race(tmp_path):
    # M6: concurrent openers of an old DB all ALTERed the same columns
    # ("duplicate column name") and of a fresh DB all created the same
    # tables ("table code already exists").
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    old, fresh = tmp_path / 'old.db', tmp_path / 'fresh.db'
    _old_db(old)
    with ProcessPoolExecutor(8, mp_context=get_context('spawn')) as pool:
        results = list(pool.map(_open, [old] * 8 + [fresh] * 8))
    assert results == ['ok'] * 16


def test_retry_locked_reraises_non_lock_errors_and_gives_up(monkeypatch):
    # todos "Bound and message-match retry_lock_commit": any OperationalError
    # used to be retried forever, turning a real error into a silent hang.
    import remake.metadata.sqlite3_backend as backend
    from remake import RemakeError

    def broken():
        raise sqlite3.OperationalError('no such table: task')

    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        backend._retry_locked(broken, 'test')

    monkeypatch.setattr(backend, 'LOCK_RETRY_SECONDS', 0)
    monkeypatch.setattr(backend, 'sleep', lambda s: None)

    def locked():
        raise sqlite3.OperationalError('database is locked')

    with pytest.raises(RemakeError, match='still locked'):
        backend._retry_locked(locked, 'test')


def test_partially_created_legacy_db_is_completed(tmp_path):
    # Review of the M5 fix: before it, executescript committed statement by
    # statement, so a first run killed part-way could leave some tables but
    # no `task`. Opening such a DB must complete the schema — including the
    # unique index on task.key — rather than fail on the tables it has.
    from remake.metadata.sqlite3_backend import SQL_SCHEMA

    db = tmp_path / 'remake.db'
    first_two = SQL_SCHEMA.split(');', 2)
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.executescript(first_two[0] + ');' + first_two[1] + ');')
        assert len(list(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"))) == 2
    with Sqlite3Backend(db) as meta:
        names = {r[0] for r in meta.conn.execute('SELECT name FROM sqlite_master')}
    assert {'task', 'meta', 'task_key_index'} <= names


# --- review 2026-09-24 M17/L14: sidecar ingest ---


def _one_task_pipeline(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'x.txt'})
    def only(outputs):
        Path(outputs['o']).write_text('ok')

    meta = Sqlite3Backend('.remake/remake.db')
    rmk = Remake(rules=[only], metadata=meta)
    rmk.finalize()
    return rmk, rmk.tasks()[0]


def test_older_sidecar_does_not_overwrite_newer_record(tmp_path, monkeypatch):
    # M17: an un-ingested FAILED sidecar from an earlier attempt reverted a
    # later direct success (e.g. `remake run-task`) on the next ingest.
    import json

    from remake.metadata import TASK_STATUS_FAILED
    from remake.metadata.sidecar import SidecarWriter, task_result_path

    rmk, task = _one_task_pipeline(tmp_path, monkeypatch)
    SidecarWriter(run_seq=1).update_task(task, TASK_STATUS_FAILED, exception='old')
    path = task_result_path(task.rule.name, task.key)
    payload = json.loads(path.read_text())
    payload['timestamp'] = '2000-01-01 00:00:00'
    path.write_text(json.dumps(payload))

    rmk.metadata.begin_invocation()
    rmk.run_task(task)  # direct write: SUCCESS, now
    assert rmk.metadata.ingest_sidecars(rmk.rules) == 1  # consumed...
    rec = rmk.metadata.get_tasks_status([task])[task.key]
    assert rec.status == TASK_STATUS_SUCCESS  # ...but did not win
    rmk.metadata.close()


def test_malformed_sidecars_are_quarantined(tmp_path, monkeypatch):
    # L14: valid JSON of the wrong shape crashed every plan/info/run; an
    # unreadable one was re-warned on every command, forever.
    from remake.metadata.sidecar import task_result_path

    rmk, task = _one_task_pipeline(tmp_path, monkeypatch)
    path = task_result_path(task.rule.name, task.key)
    path.parent.mkdir(parents=True)
    path.write_text('[]')
    other = path.with_name('ff' + path.name)
    other.write_text('{not json')
    assert rmk.metadata.ingest_sidecars(rmk.rules) == 0
    assert not path.exists() and path.with_name(path.name + '.bad').exists()
    assert not other.exists() and other.with_name(other.name + '.bad').exists()
    rmk.plan()  # no longer raises
    rmk.metadata.close()
