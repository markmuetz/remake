import sqlite3
from pathlib import Path

from loguru import logger

from remake import Remake, Sqlite3Backend, rule


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
