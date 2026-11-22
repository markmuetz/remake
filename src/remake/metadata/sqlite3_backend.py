"""SQLite metadata backend.

Schema carried over from remake2, with the task table reworked so that all
code-derived state (run source, the normalised uses/io strings) lives in the
content-addressed `code` table and task rows carry only integer FKs. Storing
the uses/io text inline per task made real DBs enormous (272 MB for 3341
tasks, 99.8% of it duplicated uses text) and made status queries scale with
task count (design_docs/records/logs_analysis/README.md). Older DBs are migrated in
place by `_add_missing_columns`.
"""
import dataclasses
import json
import random
import re
import sqlite3
from pathlib import Path
from time import monotonic, perf_counter, sleep

from loguru import logger

from ..core.exceptions import RemakeError
from ..core.scope import io_hash as compute_io_hash
from ..core.scope import raw_uses_parts
from ..core.scope import rule_uses_hash
from ..util.code_compare import CodeComparer
from .metadata_manager import (TASK_STATUS_PENDING, TASK_STATUS_SUCCESS, MetadataManager,
                               TaskRecord)

# On-disk schema version, stored in the SQLite header (`PRAGMA user_version`)
# as MAJOR * 1000 + MINOR (0.9.0 item 3). 0 is every DB written before 0.9.0:
# it is migrated in place and stamped.
#
# - MINOR changes are additive (a nullable column, a new table): an older
#   remake of the same MAJOR may still open the DB and simply ignores them.
# - MAJOR changes are not: a DB of a newer MAJOR than this code knows is
#   refused with an upgrade hint, since this version would misread or clobber
#   what it stores.
#
# Bump on every change to SQL_SCHEMA or _add_missing_columns, and add the
# migration step there. Sidecars and job specs carry their own "v" field.
SCHEMA_MAJOR = 1
SCHEMA_MINOR = 1  # 1.1 (0.10): run provenance + counts, environment, task_run
SCHEMA_VERSION = SCHEMA_MAJOR * 1000 + SCHEMA_MINOR


def split_schema_version(user_version):
    return divmod(user_version, 1000)

SQL_SCHEMA = """
CREATE TABLE code (
    id INTEGER NOT NULL,
    code TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE rule (
    id INTEGER NOT NULL,
    name VARCHAR(200) NOT NULL,
    inputs_code_id INTEGER NOT NULL,
    outputs_code_id INTEGER NOT NULL,
    run_code_id INTEGER NOT NULL,
    -- The remakefile that last defined this rule. Rules are namespaced only by
    -- name (so co-located remakefiles share this store, by design), so this
    -- records provenance and powers the duplicate-rule-name guard: a same-named
    -- rule with different code last written by a *different* remakefile is a
    -- collision, not an edit. NULL = unknown (programmatic use / pre-upgrade).
    remakefile TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY(inputs_code_id) REFERENCES code (id),
    FOREIGN KEY(outputs_code_id) REFERENCES code (id),
    FOREIGN KEY(run_code_id) REFERENCES code (id)
);

-- uses_code_id/io_code_id point at the *normalised* uses/io strings interned
-- in code (find-or-insert by exact content, so equal content means equal id
-- and the planner compares ints, never text). run_code_id points at raw
-- source, deduped only against the rule's latest version, so ids there are
-- not canonical across history — the planner AST-compares the distinct few.
CREATE TABLE task (
    id INTEGER NOT NULL,
    key VARCHAR(40) NOT NULL,
    rule_id INTEGER NOT NULL,
    run_code_id INTEGER,
    uses_code_id INTEGER,
    io_code_id INTEGER,
    run_seq INTEGER,
    last_run_timestamp TIMESTAMP,
    last_run_status INTEGER,
    exception TEXT,
    -- The LAST execution's measured resources (design_docs/
    -- resource_capture.md). Nullable and never read by the planner, so they
    -- can't become a rerun trigger and a pre-upgrade record is simply "not
    -- measured". Per-execution history is the 0.10.x stats store; these four
    -- columns are the cheap last-only slice, written by the upsert that
    -- already happens. rss_method: 'sample' | 'rusage' | NULL — which
    -- measurement produced max_rss_bytes, so consumers never compare a
    -- sampled value against a getrusage one.
    wall_s REAL,
    cpu_s REAL,
    max_rss_bytes INTEGER,
    rss_method TEXT,
    -- core.task.paths_digest of the resolved input/output paths the task
    -- last ran with (review 2026-09-24 M8): a fan-in whose callable spec
    -- now resolves to fewer/other paths reruns. NULL = not recorded.
    paths_digest TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY(rule_id) REFERENCES rule (id),
    FOREIGN KEY(run_code_id) REFERENCES code (id),
    FOREIGN KEY(uses_code_id) REFERENCES code (id),
    FOREIGN KEY(io_code_id) REFERENCES code (id)
);

CREATE UNIQUE INDEX task_key_index ON task(key);

-- Per-helper raw source for a `uses` version, for display (readable diffs in
-- `why`; change detection never reads this — it compares task.uses_code_id).
-- Keyed by the *joined-normalised-string* id rather than the rule: manifests
-- are then immutable per version, so a task's stored uses_code_id always
-- resolves to the helpers it actually ran with, however many edits later.
-- kind: 'source' (raw source, diffable) / 'value' (repr) / 'bytecode'
-- (sourceless callable — label only, display "source unavailable").
CREATE TABLE uses_manifest (
    uses_code_id INTEGER NOT NULL,
    name VARCHAR(200) NOT NULL,
    code_id INTEGER NOT NULL,
    kind VARCHAR(10) NOT NULL,
    PRIMARY KEY (uses_code_id, name),
    FOREIGN KEY(uses_code_id) REFERENCES code (id),
    FOREIGN KEY(code_id) REFERENCES code (id)
);

-- Output stats of a task's last successful execution (0.9.0 items 4/5):
-- one row per Dir output or checksummed output, and one row named '*'
-- summing a task's other (plain file) outputs, so a 1e4-task x 100-file
-- pipeline adds 1e4 rows, not 1e6. checksum: sha256 hex, opt-in.
CREATE TABLE output_stat (
    task_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    n_files INTEGER,
    total_bytes INTEGER,
    checksum TEXT,
    PRIMARY KEY (task_id, name),
    FOREIGN KEY(task_id) REFERENCES task (id)
);

-- One row per invocation that wrote (run/set-state): when, from which
-- remakefile, and the effective merged configuration it ran with (JSON) —
-- reproducibility, and the anchor for 0.10's provenance capture.
CREATE TABLE run (
    run_seq INTEGER NOT NULL PRIMARY KEY,
    started TIMESTAMP NOT NULL,
    remakefile TEXT,
    command TEXT,
    config TEXT,
    -- Provenance (0.10 item 1; remake/provenance.py). Recorded, never
    -- compared: none of these is a rerun trigger. NULL = not recorded.
    git_commit TEXT,
    git_dirty INTEGER,
    git_diff_hash TEXT,
    python_version TEXT,
    remake_version TEXT,
    env_id INTEGER,
    lockfile TEXT,
    lockfile_hash TEXT,
    host TEXT,
    user_hash TEXT,
    -- A continuation run (SLURM deferred rules) points at the run that
    -- started the chain, so one logical run's rows can be grouped.
    parent_run_seq INTEGER,
    -- Per-run counts (0.10 item 3), written when the run finishes.
    finished TIMESTAMP,
    n_planned INTEGER,
    n_run INTEGER,
    n_succeeded INTEGER,
    n_failed INTEGER,
    n_blocked_rules INTEGER,
    -- env_id -> environment(id); no FOREIGN KEY clause, so that the
    -- in-place migration (ALTER TABLE ADD COLUMN) gives the same shape.
    n_waves INTEGER
);

-- Installed-distribution lists, interned by fingerprint (one row per
-- distinct environment, however many runs share it).
CREATE TABLE environment (
    id INTEGER NOT NULL PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE,
    python_version TEXT,
    packages TEXT NOT NULL
);

-- Run history (0.10 item 3): one row per task *execution*, where `task`
-- keeps only the last. Capped per task (history.keep). env_id is set only
-- when the task ran in a different environment from the run that planned
-- it (a SLURM job); NULL means "the run's environment".
CREATE TABLE task_run (
    id INTEGER NOT NULL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    run_seq INTEGER,
    finished TIMESTAMP NOT NULL,
    status INTEGER NOT NULL,
    wall_s REAL,
    cpu_s REAL,
    max_rss_bytes INTEGER,
    rss_method TEXT,
    output_bytes INTEGER,
    reason TEXT,
    host TEXT,
    error TEXT,
    env_id INTEGER,
    FOREIGN KEY(task_id) REFERENCES task (id),
    FOREIGN KEY(env_id) REFERENCES environment (id)
);
CREATE INDEX task_run_task_index ON task_run(task_id, id);
CREATE INDEX task_run_seq_index ON task_run(run_seq);

-- Key/value store. run_seq: a monotonic counter, one value allocated per
-- `remake run`/`set-state` invocation, stamped onto every task that
-- invocation commits. The planner reruns a task when an upstream's stamp is
-- greater (a durable replacement for in-pass-only rerun propagation) — see
-- design_docs/bugs/01_durable_rerun_propagation.md.
CREATE TABLE meta (
    key TEXT NOT NULL PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT INTO meta(key, value) VALUES ('run_seq', 0);
"""


# Columns added to `run` in schema 1.1 (0.10), for in-place migration.
_RUN_COLUMNS_1_1 = (
    ('git_commit', 'TEXT'), ('git_dirty', 'INTEGER'), ('git_diff_hash', 'TEXT'),
    ('python_version', 'TEXT'), ('remake_version', 'TEXT'), ('env_id', 'INTEGER'),
    ('lockfile', 'TEXT'), ('lockfile_hash', 'TEXT'), ('host', 'TEXT'),
    ('user_hash', 'TEXT'), ('parent_run_seq', 'INTEGER'), ('finished', 'TIMESTAMP'),
    ('n_planned', 'INTEGER'), ('n_run', 'INTEGER'), ('n_succeeded', 'INTEGER'),
    ('n_failed', 'INTEGER'), ('n_blocked_rules', 'INTEGER'), ('n_waves', 'INTEGER'),
)

_PROVENANCE_KEYS = ('git_commit', 'git_dirty', 'git_diff_hash', 'python_version',
                    'remake_version', 'lockfile', 'lockfile_hash', 'host', 'user_hash')
_COUNT_KEYS = ('n_planned', 'n_run', 'n_succeeded', 'n_failed', 'n_blocked_rules',
               'n_waves')
DEFAULT_HISTORY_KEEP = 10


_TB_FRAME = re.compile(r'  File "(.+?)", line (\d+), in (.+)')


def error_signature(tb):
    """A short, message-free signature of a stored traceback for history
    rows: 'ExcType at file:line in fn' (the deepest frame), or None for no
    error. Tasks failing the same way share it."""
    if not tb:
        return None
    lines = tb.strip().splitlines()
    exc_type = lines[-1].split(':', 1)[0].strip() if lines else ''
    frames = _TB_FRAME.findall(tb)
    if not frames:
        return exc_type or None
    path, line, fn = frames[-1]
    return f'{exc_type} at {Path(path).name}:{line} in {fn}'


def _schema_statements():
    """SQL_SCHEMA as single, idempotent statements, run one by one inside an
    explicit transaction (executescript would COMMIT it first).

    Idempotent (IF NOT EXISTS / OR IGNORE) so they can run on every open: a
    DB left partially created by an older remake — whose executescript
    committed statement by statement — gets its missing tables and index
    instead of failing on the ones it has. sqlite3.complete_statement is
    tokenizer-aware, so a ';' inside a comment doesn't split a statement."""
    stmts, buf = [], ''
    for line in SQL_SCHEMA.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            stmts.append(_idempotent(buf))
            buf = ''
    assert not buf.strip(), 'SQL_SCHEMA ends with an incomplete statement'
    return stmts


def _idempotent(stmt):
    for old, new in (('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS '),
                     ('CREATE UNIQUE INDEX ', 'CREATE UNIQUE INDEX IF NOT EXISTS '),
                     ('CREATE INDEX ', 'CREATE INDEX IF NOT EXISTS '),
                     ('INSERT INTO ', 'INSERT OR IGNORE INTO ')):
        stmt = stmt.replace(old, new)
    return stmt


# How long a writer keeps retrying a locked DB before giving up. Long enough
# to sit out another process's one-time migration or a big sidecar ingest;
# finite, so a DB that stays locked becomes an error rather than a hang.
LOCK_RETRY_SECONDS = 600


def _quarantine(path, why):
    """Move a bad sidecar aside (`*.json.bad`, outside the ingest glob) with
    one warning, instead of failing or re-warning on every command."""
    bad = path.with_name(path.name + '.bad')
    try:
        path.replace(bad)
    except FileNotFoundError:
        return  # another process got there first
    logger.warning(f'Quarantined {why} sidecar result: {bad}')


def _is_lock_error(exc):
    msg = str(exc).lower()
    return 'locked' in msg or 'busy' in msg


def _retry_locked(fn, what):
    """Call fn(), retrying with capped exponential backoff while SQLite
    reports lock contention. Any other OperationalError ("no such table",
    disk full, malformed schema) is re-raised at once instead of being
    retried forever (todos: "Bound and message-match retry_lock_commit")."""
    deadline = monotonic() + LOCK_RETRY_SECONDS
    nattempts = 1
    while True:
        try:
            return fn()
        except sqlite3.OperationalError as oe:
            if not _is_lock_error(oe):
                raise
            if monotonic() >= deadline:
                raise RemakeError(
                    f'{what}: database still locked after {LOCK_RETRY_SECONDS}s '
                    f'— is another remake process stuck? ({oe})'
                ) from oe
            logger.debug(f'{what}: {oe} (attempt {nattempts}); retrying')
        nattempts += 1
        sleep(min(30, 2**nattempts * random.random()))


def retry_lock_commit(fn):
    """Run fn in an EXCLUSIVE transaction, retrying with backoff on lock
    contention (concurrent workers/SLURM jobs share the DB)."""

    def inner(self, *args, **kwargs):
        def attempt():
            with self.conn:
                self.conn.execute('BEGIN EXCLUSIVE')
                ret = fn(self, *args, **kwargs)
                self.conn.commit()
            return ret

        return _retry_locked(attempt, fn.__name__)

    return inner



def _config_json(config):
    """The config as JSON for the run record. Provenance must never fail a
    command: keys JSON can't take (tuples, enums) or can't sort (mixed int
    and str) are stringified (ultrareview 2026-11-21)."""
    try:
        return json.dumps(config, sort_keys=True, default=repr)
    except (TypeError, ValueError):
        def norm(v):
            if isinstance(v, dict):
                return {str(k): norm(x) for k, x in v.items()}
            if isinstance(v, (list, tuple)):
                return [norm(x) for x in v]
            return v
        try:
            return json.dumps(norm(config), sort_keys=True, default=repr)
        except (TypeError, ValueError):
            return None

class _KeyOnly:
    """Stand-in task for a lookup by key alone."""
    legacy_key = None

    def __init__(self, key):
        self.key = key


class Sqlite3Backend(MetadataManager):
    def __init__(self, dbloc='.remake/remake.db'):
        self.dbloc = str(dbloc)
        self.code_comparer = CodeComparer()
        if self.dbloc != ':memory:':
            Path(self.dbloc).parent.mkdir(parents=True, exist_ok=True)
        # No detect_types: timestamps are read as plain strings (TaskRecord
        # .timestamp), and the implicit converter is deprecated in 3.12.
        self.conn = sqlite3.connect(self.dbloc)
        try:
            self._init_schema()
        except sqlite3.DatabaseError as e:
            self.conn.close()
            if isinstance(e, sqlite3.OperationalError):
                raise
            # e.g. "file is not a database": a corrupt or foreign file.
            raise RemakeError(
                f'{self.dbloc} is not a usable remake database ({e}); move it '
                f'aside to start a fresh one'
            ) from None
        except BaseException:
            self.conn.close()
            raise
        self.conn.isolation_level = 'EXCLUSIVE'
        # rule name -> (rule_id, run_code_id, uses_code_id, io_code_id):
        # the rule's row plus this invocation's current interned code ids.
        self.rule_ids = {}
        # Lazily allocated once per process (= per invocation); shared by every
        # task this invocation commits. See the `meta` table comment.
        self._run_seq = None
        # history.keep from the effective config (Remake sets it); None =
        # DEFAULT_HISTORY_KEEP.
        self.history_keep = None

    def close(self):
        self.conn.close()

    # Unlike sqlite3.Connection's own `with` (transaction commit/rollback),
    # this scopes the backend's lifetime: the connection closes on exit.
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        # sqlite3's finalizer emits a ResourceWarning for an unclosed
        # connection at an arbitrary later gc point; close here so a backend
        # that was never explicitly closed doesn't trip that.
        conn = getattr(self, 'conn', None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _init_schema(self):
        """Create the schema, or migrate an existing DB, in ONE exclusive
        transaction (review 2026-09-24 M5/M6).

        Whether to create is decided by the presence of the `task` table,
        checked inside the lock — not by the file existing: sqlite3.connect
        creates the file before any schema is written, so a first run killed
        mid-create (disk/quota full, Ctrl-C) used to leave a 0-byte file that
        bricked every later command. Holding the lock across check + DDL also
        serialises concurrent first openers (continuation jobs, `info` next
        to `run`), which used to race to ALTER the same columns; and a crash
        mid-migration now rolls back cleanly and is redone on the next open,
        instead of leaving a half-migrated DB that is skipped forever."""
        conn = self.conn
        conn.isolation_level = None  # explicit transaction control below
        _retry_locked(lambda: conn.execute('BEGIN EXCLUSIVE'), 'opening the database')
        try:
            (found_version,) = conn.execute('PRAGMA user_version').fetchone()
            found_major, found_minor = split_schema_version(found_version)
            if found_major > SCHEMA_MAJOR:
                raise RemakeError(
                    f'{self.dbloc} was written by a newer remake (schema '
                    f'{found_major}.{found_minor}; this remake understands '
                    f'{SCHEMA_MAJOR}.x). Upgrade remake to use it (e.g. '
                    f'`pip install -U remake` or `uv add remake@latest`): this '
                    f'version would misread or overwrite what it stores.'
                )
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'task' not in tables and self.dbloc != ':memory:':
                logger.info(f'Creating sqlite3 database: {self.dbloc}')
            # Old-shape tables that exist are left alone here (IF NOT EXISTS)
            # and brought up to date column by column below.
            for stmt in _schema_statements():
                conn.execute(stmt)
            needs_vacuum = self._add_missing_columns()
            # Never stamp *down*: a newer minor's additive changes stay valid.
            if found_version < SCHEMA_VERSION:
                if found_version and 'task' in tables:
                    logger.info(f'Upgrading database schema {found_version} -> '
                                f'{SCHEMA_VERSION}')
                # Transactional: rolls back with the migration if it fails.
                conn.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')
            conn.execute('COMMIT')
        except BaseException:
            # SQLite may already have rolled back by itself (disk full, I/O
            # error); an unconditional ROLLBACK would then raise "no
            # transaction is active" and hide the real error.
            if conn.in_transaction:
                conn.execute('ROLLBACK')
            raise
        if needs_vacuum:
            # VACUUM can't run inside a transaction, so it follows the
            # (committed) migration. Best effort: if another process holds
            # the DB it is skipped — the space is reclaimed on a later VACUUM.
            logger.info('Vacuuming (one-time; reclaims the inline-hash space)')
            try:
                conn.execute('VACUUM')
            except sqlite3.OperationalError as e:
                logger.warning(f'VACUUM skipped ({e}); the DB is correct, just larger')

    def _add_missing_columns(self):
        """Lightweight forward-compat for columns added after a DB was first
        created (still no general migration support — see the module docstring).
        A pre-existing record left with io_code_id/run_seq NULL is treated as
        'not yet tracked' by the planner, so upgrading does not force a mass
        rerun. Runs inside _init_schema's exclusive transaction: no commits
        here. Returns True when a VACUUM should follow."""
        needs_vacuum = False
        cols = {row[1] for row in self.conn.execute('PRAGMA table_info(task)')}
        if 'run_seq' not in cols:
            logger.info('Adding task.run_seq column to existing DB')
            self.conn.execute('ALTER TABLE task ADD COLUMN run_seq INTEGER')
        if 'uses_code_id' not in cols:
            self._migrate_inline_hashes_to_code_ids(cols)
            needs_vacuum = True
        # Resource-capture columns (0.9.0). Additive and nullable: an existing
        # DB gains NULLs and nothing reruns.
        for col, coltype in (('wall_s', 'REAL'), ('cpu_s', 'REAL'),
                             ('max_rss_bytes', 'INTEGER'), ('rss_method', 'TEXT'),
                             ('paths_digest', 'TEXT')):
            if col not in cols:
                logger.info(f'Adding task.{col} column to existing DB')
                self.conn.execute(f'ALTER TABLE task ADD COLUMN {col} {coltype}')
        rule_cols = {row[1] for row in self.conn.execute('PRAGMA table_info(rule)')}
        if 'remakefile' not in rule_cols:
            logger.info('Adding rule.remakefile column to existing DB')
            self.conn.execute('ALTER TABLE rule ADD COLUMN remakefile TEXT')
        tables = {row[0] for row in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if 'uses_manifest' not in tables:
            # No backfill possible (historical raw sources are gone); records
            # predating the table degrade to the manifest-less `why` message.
            logger.info('Adding uses_manifest table to existing DB')
            self.conn.execute(
                'CREATE TABLE uses_manifest ('
                '    uses_code_id INTEGER NOT NULL, name VARCHAR(200) NOT NULL, '
                '    code_id INTEGER NOT NULL, kind VARCHAR(10) NOT NULL, '
                '    PRIMARY KEY (uses_code_id, name))')
        if 'output_stat' not in tables:
            logger.info('Adding output_stat table to existing DB')
            self.conn.execute(
                'CREATE TABLE output_stat (task_id INTEGER NOT NULL, name TEXT NOT NULL, '
                'n_files INTEGER, total_bytes INTEGER, checksum TEXT, '
                'PRIMARY KEY (task_id, name))')
        if 'run' not in tables:
            logger.info('Adding run table to existing DB')
            self.conn.execute(
                'CREATE TABLE run (run_seq INTEGER NOT NULL PRIMARY KEY, '
                'started TIMESTAMP NOT NULL, remakefile TEXT, command TEXT, config TEXT)')
        run_cols = {row[1] for row in self.conn.execute('PRAGMA table_info(run)')}
        for col, coltype in _RUN_COLUMNS_1_1:
            if col not in run_cols:
                self.conn.execute(f'ALTER TABLE run ADD COLUMN {col} {coltype}')
        if 'environment' not in tables or 'task_run' not in tables:
            logger.info('Adding environment/task_run tables to existing DB')
            # SQL_SCHEMA's (idempotent) CREATEs have already made them;
            # nothing to backfill: history starts now.
        if 'meta' not in tables:
            logger.info('Adding meta table to existing DB')
            self.conn.execute(
                'CREATE TABLE meta (key TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL)')
            self.conn.execute("INSERT INTO meta(key, value) VALUES ('run_seq', 0)")
        return needs_vacuum

    def _migrate_inline_hashes_to_code_ids(self, cols):
        """One-time in-place migration: the old task.uses_hash/io_hash columns
        stored the full normalised uses/io strings inline per row — duplicated
        across every task of a rule (measured at 99.8% of a 272 MB field DB).
        Intern each distinct value into `code` once, point integer FKs at it,
        drop the text columns; the caller VACUUMs afterwards to return the
        space."""
        logger.info('Migrating task.uses_hash/io_hash to code-table FKs')
        self.conn.execute('ALTER TABLE task ADD COLUMN uses_code_id INTEGER')
        self.conn.execute('ALTER TABLE task ADD COLUMN io_code_id INTEGER')
        if 'uses_hash' in cols:
            # Backfill uses: NULL and '' both mean "empty uses" — intern ''.
            self.conn.execute(
                "INSERT INTO code(code) "
                "SELECT DISTINCT COALESCE(uses_hash, '') FROM task "
                "WHERE COALESCE(uses_hash, '') NOT IN (SELECT code FROM code)")
            self.conn.execute(
                'UPDATE task SET uses_code_id = ('
                "    SELECT min(id) FROM code WHERE code = COALESCE(task.uses_hash, ''))")
        if 'io_hash' in cols:
            # Backfill io: NULL means pre-upgrade/not-tracked and must stay
            # NULL (the planner skips the io comparison for those records).
            self.conn.execute(
                'INSERT INTO code(code) '
                'SELECT DISTINCT io_hash FROM task '
                'WHERE io_hash IS NOT NULL AND io_hash NOT IN (SELECT code FROM code)')
            self.conn.execute(
                'UPDATE task SET io_code_id = ('
                '    SELECT min(id) FROM code WHERE code = task.io_hash) '
                'WHERE io_hash IS NOT NULL')
        for col in ('uses_hash', 'io_hash'):
            if col not in cols:
                continue
            try:
                self.conn.execute(f'ALTER TABLE task DROP COLUMN {col}')
            except sqlite3.OperationalError:
                # DROP COLUMN needs SQLite >= 3.35; NULLing the column still
                # frees the space (after the VACUUM below) and nothing reads
                # it any more.
                self.conn.execute(f'UPDATE task SET {col} = NULL')

    @retry_lock_commit
    def _allocate_run_seq(self):
        self.conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'run_seq'")
        (value,) = self.conn.execute(
            "SELECT value FROM meta WHERE key = 'run_seq'").fetchone()
        return value

    def begin_invocation(self):
        """Allocate a fresh run_seq for a new logical invocation (a `run()` or
        a `set-state`), so repeated invocations on one backend instance still
        advance — the CLI starts a fresh process each time, but programmatic
        callers reuse the object."""
        self._run_seq = self._allocate_run_seq()

    def current_run_seq(self):
        """This invocation's run_seq, allocating it on first use. Every task
        committed in the invocation shares the value, so a single run/set-state
        stamps one logical 'wave' (cross-node-safe: assigned here, not read
        from a compute node's clock)."""
        if self._run_seq is None:
            self._run_seq = self._allocate_run_seq()
        return self._run_seq

    @retry_lock_commit
    def _record_run(self, run_seq, remakefile, command, config_json, provenance,
                    parent_run_seq):
        prov = provenance or {}
        env_id = None
        if prov.get('env_fingerprint'):
            env_id = self._intern_environment(
                prov['env_fingerprint'], prov.get('python_version'),
                prov.get('env_packages') or [])
        # An upsert, not INSERT OR REPLACE: a second record_run in the same
        # invocation must not null the columns the first (or finish_run) set.
        values = {
            'remakefile': remakefile, 'command': command, 'config': config_json,
            'env_id': env_id, 'parent_run_seq': parent_run_seq,
            **{k: (int(prov[k]) if k == 'git_dirty' and prov.get(k) is not None
                   else prov.get(k)) for k in _PROVENANCE_KEYS},
        }
        cols = ', '.join(values)
        marks = ', '.join('?' * len(values))
        updates = ', '.join(f'{c} = COALESCE(excluded.{c}, {c})' for c in values)
        self.conn.execute(
            f"INSERT INTO run(run_seq, started, {cols}) "
            f"VALUES (?, datetime('now'), {marks}) "
            f"ON CONFLICT(run_seq) DO UPDATE SET {updates}",
            (run_seq, *values.values()))

    def record_run(self, *, remakefile=None, command=None, config=None,
                   provenance=None, parent_run_seq=None):
        """Record this invocation's `run` row: what ran, with which config
        and (0.10) provenance from `remake.provenance.collect`."""
        self._record_run(self.current_run_seq(), remakefile, command,
                         _config_json(config) if config is not None else None,
                         provenance, parent_run_seq)

    @retry_lock_commit
    def finish_run(self, counts):
        """Stamp the per-run counts (n_planned, n_run, n_succeeded, n_failed,
        n_blocked_rules, n_waves) and the finish time on this run's row, and
        prune the history of the tasks it ran."""
        run_seq = self.current_run_seq()
        sets = ', '.join(f'{k} = ?' for k in _COUNT_KEYS)
        self.conn.execute(
            f"UPDATE run SET finished = datetime('now'), {sets} WHERE run_seq = ?",
            (*(counts.get(k) for k in _COUNT_KEYS), run_seq))
        self._prune_history(run_seq=run_seq)

    def _intern_environment(self, fingerprint, python_version, packages):
        """Find-or-insert an environment by fingerprint; its id."""
        row = self.conn.execute(
            'SELECT id FROM environment WHERE fingerprint = ?', (fingerprint,)).fetchone()
        if row is not None:
            return row[0]
        cur = self.conn.execute(
            'INSERT INTO environment(fingerprint, python_version, packages) '
            'VALUES (?, ?, ?)', (fingerprint, python_version, json.dumps(list(packages))))
        return cur.lastrowid

    _RUN_FIELDS = ('run_seq', 'started', 'remakefile', 'command', 'config',
                   *(c for c, _ in _RUN_COLUMNS_1_1))

    def _run_from_row(self, row):
        rec = dict(zip(self._RUN_FIELDS, row))
        rec['config'] = json.loads(rec['config']) if rec['config'] else None
        if rec.get('git_dirty') is not None:
            rec['git_dirty'] = bool(rec['git_dirty'])
        return rec

    def get_run(self, run_seq):
        """The `run` row for run_seq as a dict, or None."""
        row = self.conn.execute(
            f'SELECT {", ".join(self._RUN_FIELDS)} FROM run WHERE run_seq = ?',
            (run_seq,)).fetchone()
        return None if row is None else self._run_from_row(row)

    def get_runs(self, limit=20):
        """The most recent `run` rows, newest first."""
        rows = self.conn.execute(
            f'SELECT {", ".join(self._RUN_FIELDS)} FROM run '
            'ORDER BY run_seq DESC LIMIT ?', (limit,))
        return [self._run_from_row(row) for row in rows]

    def get_environment(self, env_id):
        """{'fingerprint', 'python_version', 'packages'} for an environment id."""
        row = self.conn.execute(
            'SELECT fingerprint, python_version, packages FROM environment WHERE id = ?',
            (env_id,)).fetchone()
        if row is None:
            return None
        return {'fingerprint': row[0], 'python_version': row[1],
                'packages': json.loads(row[2])}

    _HISTORY_FIELDS = ('run_seq', 'finished', 'status', 'wall_s', 'cpu_s',
                       'max_rss_bytes', 'rss_method', 'output_bytes', 'reason',
                       'host', 'error', 'env_id')

    def get_history(self, task, limit=None):
        """A task's executions, newest first: dicts of _HISTORY_FIELDS plus
        'run_env_id' (the planning run's environment) for comparison."""
        sql = (f'SELECT {", ".join("h." + f for f in self._HISTORY_FIELDS)}, r.env_id '
               'FROM task_run h JOIN task t ON h.task_id = t.id '
               'LEFT JOIN run r ON r.run_seq = h.run_seq '
               'WHERE t.key = ? ORDER BY h.id DESC')
        params = [task.key]
        if limit is not None:
            sql += ' LIMIT ?'
            params.append(limit)
        out = []
        for row in self.conn.execute(sql, params):
            rec = dict(zip(self._HISTORY_FIELDS, row[:-1]))
            rec['run_env_id'] = row[-1]
            out.append(rec)
        return out

    def _history_keep(self):
        return self.history_keep if self.history_keep is not None else DEFAULT_HISTORY_KEEP

    def _prune_history(self, run_seq=None, keep=None):
        """Keep the newest `keep` executions per task: for the tasks run_seq
        touched (once per run, not per insert), or for every task. Returns
        the number of rows deleted."""
        keep = self._history_keep() if keep is None else keep
        scope = ('WHERE task_id IN (SELECT task_id FROM task_run WHERE run_seq = ?)'
                 if run_seq is not None else '')
        params = (run_seq,) if run_seq is not None else ()
        cur = self.conn.execute(
            'DELETE FROM task_run WHERE id IN ('
            '  SELECT id FROM ('
            '    SELECT id, ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY id DESC) AS n '
            f'   FROM task_run {scope}) WHERE n > ?)', (*params, keep))
        return cur.rowcount

    @retry_lock_commit
    def prune_history(self, keep=None):
        """`remake history --prune`: apply the cap to every task now."""
        return self._prune_history(keep=keep)

    def _write_history(self, key, status, run_seq, resources, output_stats, exception,
                       execution, timestamp=None):
        """Append one execution to task_run (an execution, not a bulk state
        change: callers only write when resources were measured)."""
        (task_id,) = self.conn.execute('SELECT id FROM task WHERE key = ?', (key,)).fetchone()
        res = resources or {}
        ex = execution or {}
        output_bytes = None
        if output_stats:
            output_bytes = sum((st.get('total_bytes') or 0) for st in output_stats.values())
        env_id = None
        env = ex.get('env')
        if env and env.get('fingerprint'):
            env_id = self._intern_environment(
                env['fingerprint'], env.get('python_version'), env.get('packages') or [])
        self.conn.execute(
            'INSERT INTO task_run(task_id, run_seq, finished, status, wall_s, cpu_s, '
            '    max_rss_bytes, rss_method, output_bytes, reason, host, error, env_id) '
            "VALUES (?, ?, COALESCE(?, datetime('now')), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, run_seq, timestamp, status, res.get('wall_s'), res.get('cpu_s'),
             res.get('max_rss_bytes'), res.get('rss_method'), output_bytes,
             ex.get('reason'), ex.get('host'), error_signature(exception), env_id))

    def _insert_code(self, code):
        cur = self.conn.execute('INSERT INTO code(code) VALUES (?)', (code,))
        return cur.lastrowid

    def _intern_code(self, code):
        """Find-or-insert: the id of the row whose content is exactly `code`.
        Content-addressing makes ids canonical — the same string always
        resolves to the same id, so unchanged-ness is id equality. (min(id)
        because historic inserts may have left duplicate content.)"""
        (existing,) = self.conn.execute(
            'SELECT min(id) FROM code WHERE code = ?', (code,)).fetchone()
        if existing is not None:
            return existing
        return self._insert_code(code)

    def _ensure_uses_manifest(self, uses_code_id, uses):
        """Record the per-helper raw sources behind a uses version, once.
        Write-once per uses_code_id: the id is derived from the normalised
        content, so an existing manifest for it is already correct (at worst
        it holds a cosmetic raw variant with the same structure). Helpers are
        interned individually, so editing one of N shares the other N-1."""
        (n,) = self.conn.execute(
            'SELECT count(*) FROM uses_manifest WHERE uses_code_id = ?',
            (uses_code_id,)).fetchone()
        if n or not uses:
            return
        for name, (raw, kind) in raw_uses_parts(uses).items():
            self.conn.execute(
                'INSERT OR IGNORE INTO uses_manifest(uses_code_id, name, code_id, kind) '
                'VALUES (?, ?, ?, ?)',
                (uses_code_id, name, self._intern_code(raw), kind))

    @retry_lock_commit
    def restamp_codes(self, restamp):
        for (rule, kind), old_ids in restamp.items():
            if rule.name not in self.rule_ids or not old_ids:
                continue
            if kind == 'paths':
                # Backfill: {task key: digest} for records predating it.
                self.conn.executemany(
                    'UPDATE task SET paths_digest = ? WHERE key = ? AND paths_digest IS NULL',
                    [(d, k) for k, d in old_ids.items() if d is not None])
                continue
            rule_id, _, uses_code_id, io_code_id = self.rule_ids[rule.name]
            column, new_id = (('uses_code_id', uses_code_id) if kind == 'uses'
                              else ('io_code_id', io_code_id))
            old = sorted(old_ids)
            placeholders = ','.join('?' * len(old))
            cur = self.conn.execute(
                f'UPDATE task SET {column} = ? WHERE rule_id = ? '
                f'AND {column} IN ({placeholders})', [new_id, rule_id, *old])
            logger.debug('restamped {} {} record(s) of {} to the 0.9 rendering',
                         cur.rowcount, kind, rule.name)

    def get_uses_manifest(self, uses_code_id):
        rows = self.conn.execute(
            'SELECT m.name, c.code, m.kind '
            'FROM uses_manifest m JOIN code c ON m.code_id = c.id '
            'WHERE m.uses_code_id = ?', (uses_code_id,))
        return {name: (raw, kind) for name, raw, kind in rows}

    @retry_lock_commit
    def _ensure_rule(self, rule, remakefile=None):
        row = self.conn.execute(
            'SELECT id, inputs_code_id, outputs_code_id, run_code_id, remakefile '
            'FROM rule WHERE name = ?',
            (rule.name,),
        ).fetchone()
        source = rule.source
        # Current uses/io state, interned once per rule per invocation: tasks
        # committed this invocation point at these ids, and the planner
        # detects change by comparing a record's stored ids against them.
        uses_code_id = self._intern_code(rule_uses_hash(rule))
        io_code_id = self._intern_code(compute_io_hash(rule))
        self._ensure_uses_manifest(uses_code_id, rule.uses)
        if row is None:
            code_ids = {part: self._intern_code(source[part]) for part in source}
            cur = self.conn.execute(
                'INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id, '
                '                 remakefile) VALUES (?, ?, ?, ?, ?)',
                (rule.name, code_ids['inputs'], code_ids['outputs'], code_ids['run'],
                 remakefile),
            )
            self.rule_ids[rule.name] = (
                cur.lastrowid, code_ids['run'], uses_code_id, io_code_id)
            return True, False

        rule_id, *code_id_list, stored_remakefile = row
        code_ids = dict(zip(['inputs', 'outputs', 'run'], code_id_list))
        changed = False
        for part in ['inputs', 'outputs', 'run']:
            (stored,) = self.conn.execute(
                'SELECT code FROM code WHERE id = ?', (code_ids[part],)
            ).fetchone()
            if not self.code_comparer(stored, source[part]):
                # Intern rather than blind-insert: reverting an edit maps back
                # to the original row, so old task records compare equal again.
                code_ids[part] = self._intern_code(source[part])
                changed = True
        # Duplicate-rule-name guard: in a shared .remake/ store, a same-named
        # rule whose code differs and was last written by a *different* known
        # remakefile is a collision (two pipelines clobbering each other's
        # state/logs/jobs), not an ordinary edit. See discussion in
        # design_docs/discussion.md ("single .remake/ per directory").
        if (changed and remakefile and stored_remakefile
                and stored_remakefile != remakefile):
            logger.warning(
                "rule '{}' is defined in both '{}' and '{}', which share the "
                "same .remake/ store in this directory. They will overwrite each "
                "other's recorded state (causing spurious reruns) and clash on "
                ".remake/jobs/{}.* and the per-task log/SLURM-output dirs. "
                "Rename one rule, run them in separate directories, or — if it is "
                "meant to be the same rule — import it from a shared module. "
                "(If the rule is intentionally shared, this fires after an edit "
                "and can be ignored.)",
                rule.name, remakefile, stored_remakefile, rule.name,
            )
        if changed:
            logger.trace('rule {}: stored code changed', rule.name)
            self.conn.execute(
                'UPDATE rule SET inputs_code_id = ?, outputs_code_id = ?, '
                '                run_code_id = ?, remakefile = ? WHERE id = ?',
                (code_ids['inputs'], code_ids['outputs'], code_ids['run'],
                 remakefile if remakefile is not None else stored_remakefile, rule_id),
            )
        elif remakefile is not None and remakefile != stored_remakefile:
            # Provenance moved (e.g. a shared rule run from another file) but the
            # code is unchanged: record the new owner without a spurious rerun.
            self.conn.execute(
                'UPDATE rule SET remakefile = ? WHERE id = ?', (remakefile, rule_id))
        self.rule_ids[rule.name] = (rule_id, code_ids['run'], uses_code_id, io_code_id)
        return row is None, changed

    def ensure_rules(self, rules, remakefile=None):
        ninserted = nchanged = 0
        for rule in rules:
            inserted, changed = self._ensure_rule(rule, remakefile)
            ninserted += inserted
            nchanged += changed
        logger.bind(
            event='ensure_rules', nrules=len(rules), nnew=ninserted,
            nchanged=nchanged,
        ).debug(
            'ensured {} rule(s): {} new, {} with changed code',
            len(rules), ninserted, nchanged,
        )

    # Below SQLite's historical SQLITE_MAX_VARIABLE_NUMBER default of 999.
    SELECT_CHUNK = 900

    def get_tasks_status(self, tasks):
        # Deliberately no JOIN on code: hauling the full source text per task
        # made this query scale with task count × source size (256× the
        # distinct bytes in the field — logs_analysis §1.2). Rows carry only
        # ids; callers resolve the few distinct ones via get_codes.
        start = perf_counter()
        records = {}
        tasks = list(tasks)
        keys = [task.key for task in tasks]
        nchunks = (len(keys) + self.SELECT_CHUNK - 1) // self.SELECT_CHUNK
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            rows = self.conn.execute(
                'SELECT key, last_run_status, last_run_timestamp, '
                '       run_code_id, uses_code_id, io_code_id, run_seq, exception, '
                '       wall_s, cpu_s, max_rss_bytes, rss_method, paths_digest '
                f'FROM task WHERE key IN ({placeholders})',
                chunk,
            ).fetchall()
            for (key, status, timestamp, run_code_id, uses_code_id,
                 io_code_id, run_seq, exception,
                 wall_s, cpu_s, max_rss_bytes, rss_method, paths_digest) in rows:
                records[key] = TaskRecord(
                    key=key,
                    status=status,
                    timestamp=timestamp,
                    run_code_id=run_code_id,
                    uses_code_id=uses_code_id,
                    exception=exception or '',
                    io_code_id=io_code_id,  # None for pre-upgrade records
                    run_seq=run_seq,  # None for pre-upgrade/never-stamped records
                    # None where nothing was measured (pre-upgrade record,
                    # capture off, bulk set-state).
                    wall_s=wall_s,
                    cpu_s=cpu_s,
                    max_rss_bytes=max_rss_bytes,
                    rss_method=rss_method,
                    paths_digest=paths_digest,
                )
        # Records stored under a pre-canonical key (M19: numpy/enum matrix
        # values) are found under that key and returned under the new one;
        # the next commit of the task re-keys the row (_upsert_task).
        legacy = {t.legacy_key: t.key for t in tasks
                  if t.legacy_key and t.key not in records}
        if legacy:
            found = self.get_tasks_status(
                [_KeyOnly(k) for k in legacy])
            for old_key, rec in found.items():
                records[legacy[old_key]] = dataclasses.replace(rec, key=legacy[old_key])
        elapsed = perf_counter() - start
        # ~1857 of these dominated a field DEBUG log (logs_analysis §3.3):
        # only slow outliers earn DEBUG; the rest are TRACE. Fields are bound
        # (not just interpolated) so the JSONL sink carries them as data.
        log = logger.bind(
            event='status_query', ntasks=len(keys), nchunks=nchunks,
            nfound=len(records), seconds=round(elapsed, 6),
        )
        log_at = log.debug if elapsed > 0.1 else log.trace
        log_at(
            'queried status of {} task(s) in {} chunk(s), {} found, in {:.3f}s',
            len(keys), nchunks, len(records), elapsed,
        )
        return records

    def get_codes(self, code_ids):
        ids = sorted({cid for cid in code_ids if cid is not None})
        codes = {}
        for i in range(0, len(ids), self.SELECT_CHUNK):
            chunk = ids[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            rows = self.conn.execute(
                f'SELECT id, code FROM code WHERE id IN ({placeholders})', chunk)
            codes.update(dict(rows))
        return codes

    def ingest_sidecars(self, rules):
        """Absorb pending sidecar results (written by run-array-task) into
        the DB — one batched transaction for the lot, sidecars deleted
        after commit. Cheap when there is nothing to ingest."""
        from .sidecar import RESULTS_ROOT

        start = perf_counter()
        pending = []
        for rule in rules:
            rule_dir = RESULTS_ROOT / rule.name
            if not rule_dir.is_dir():
                continue
            for path in sorted(rule_dir.glob('*/*.json')):
                key = path.parent.name + path.stem
                try:
                    payload = json.loads(path.read_text())
                except FileNotFoundError:
                    continue  # another process ingested it first
                except (json.JSONDecodeError, UnicodeDecodeError):
                    _quarantine(path, 'unreadable')
                    continue
                if not (isinstance(payload, dict)
                        and isinstance(payload.get('status'), int)):
                    # Valid JSON of the wrong shape (hand-edited, another
                    # remake version) used to crash every plan/info/run
                    # inside the ingest transaction (review 2026-09-24 L14).
                    _quarantine(path, 'malformed (no integer "status")')
                    continue
                logger.trace('sidecar {} ({}): {}', key, rule.name, payload.get('status'))
                pending.append((rule, key, payload, path))
        if not pending:
            return 0

        self._ingest_records(pending)
        # Delete only after a successful commit; double ingestion of a
        # sidecar that survives a crash here is harmless (upsert).
        for *_, path in pending:
            path.unlink(missing_ok=True)
        elapsed = perf_counter() - start
        logger.bind(
            event='ingest', ningested=len(pending), seconds=round(elapsed, 6),
        ).debug(f'Ingested {len(pending)} sidecar result(s) in {elapsed:.3f}s')
        return len(pending)

    @retry_lock_commit
    def _ingest_records(self, pending):
        # Sidecars carry the uses/io strings as text (the compute node has no
        # DB to intern into); intern here, memoised — a batch's payloads are
        # near-always identical within a rule.
        intern_memo = {}

        def intern(text):
            if text not in intern_memo:
                intern_memo[text] = self._intern_code(text)
            return intern_memo[text]

        for rule, key, payload, _ in pending:
            rule_id, run_code_id, _, cur_io_code_id = self.rule_ids[rule.name]
            io_text = payload.get('io_hash')
            run_text = payload.get('run_hash')
            # A sidecar always records an execution, so its resources replace
            # the columns verbatim (a pre-0.9 sidecar has none: NULLs).
            res = payload.get('resources') or {}
            cur = self.conn.execute(
                'INSERT INTO task(key, rule_id, run_code_id, uses_code_id, io_code_id, '
                '                 run_seq, last_run_timestamp, last_run_status, exception, '
                '                 wall_s, cpu_s, max_rss_bytes, rss_method, paths_digest) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(key) DO UPDATE SET '
                '    run_code_id = excluded.run_code_id, '
                '    uses_code_id = excluded.uses_code_id, '
                '    io_code_id = excluded.io_code_id, '
                # NULL (a legacy sidecar): keep the stored run_seq.
                '    run_seq = COALESCE(excluded.run_seq, run_seq), '
                '    last_run_timestamp = excluded.last_run_timestamp, '
                '    wall_s = excluded.wall_s, '
                '    cpu_s = excluded.cpu_s, '
                '    max_rss_bytes = excluded.max_rss_bytes, '
                '    rss_method = excluded.rss_method, '
                '    paths_digest = excluded.paths_digest, '
                '    last_run_status = excluded.last_run_status, '
                '    exception = excluded.exception '
                # Never let an older result overwrite a newer one (review
                # 2026-09-24 M17): a sidecar left un-ingested (SLURM element,
                # dead multiproc parent) must not revert a later write such as
                # `remake run-task`. Ordered by run_seq — allocated centrally
                # per invocation — not wall clocks, which skew between compute
                # and login nodes. Without a run_seq on either side (legacy
                # records/sidecars) the sidecar applies, as before.
                'WHERE task.run_seq IS NULL OR excluded.run_seq IS NULL '
                '   OR excluded.run_seq >= task.run_seq',
                (
                    key,
                    rule_id,
                    # Pre-run_hash sidecar: fall back to the rule's current
                    # run state (mis-attributes the run source if the rule was
                    # edited between run and ingest — the bug the run_hash
                    # field exists to fix).
                    intern(run_text) if run_text else run_code_id,
                    intern(payload.get('uses_hash', '')),
                    # Pre-io_hash sidecar: fall back to the rule's current io
                    # state (was compute_io_hash(rule), interned at ensure).
                    intern(io_text) if io_text else cur_io_code_id,
                    # run_seq is allocated on the submit node and threaded
                    # through the job spec into the sidecar, so all array
                    # elements of one submission share it regardless of node.
                    payload.get('run_seq'),
                    payload.get('timestamp'),
                    payload['status'],
                    payload.get('exception', ''),
                    res.get('wall_s'),
                    res.get('cpu_s'),
                    res.get('max_rss_bytes'),
                    res.get('rss_method'),
                    payload.get('paths_digest'),
                ),
            )
            # Only when the sidecar's result was applied (not an older one
            # the run_seq guard skipped); a successful result's stats replace
            # the last run's.
            if cur.rowcount and payload['status'] == TASK_STATUS_SUCCESS and 'output_stats' in payload:
                self._write_output_stats(key, payload['output_stats'])
            # Every sidecar is an execution (history is append-only, so an
            # older one the guard skipped above is still history).
            self._write_history(key, payload['status'], payload.get('run_seq'), res,
                                payload.get('output_stats'), payload.get('exception', ''),
                                payload.get('execution'), payload.get('timestamp'))
        for run_seq in {payload.get('run_seq') for _, _, payload, _ in pending}:
            if run_seq is not None:
                self._prune_history(run_seq=run_seq)

    def update_task(self, task, status, exception='', resources=None, output_stats=None,
                    execution=None):
        # Allocate run_seq (own txn) before opening the upsert's EXCLUSIVE txn.
        self._commit_updates(
            [task], status, exception, self.current_run_seq(), resources, output_stats,
            execution)

    def _write_output_stats(self, key, output_stats):
        (task_id,) = self.conn.execute('SELECT id FROM task WHERE key = ?', (key,)).fetchone()
        self.conn.execute('DELETE FROM output_stat WHERE task_id = ?', (task_id,))
        rows, plain_n, plain_bytes = [], 0, 0
        for name, st in sorted(output_stats.items()):
            if st.get('checksum') is None and not st.get('dir'):
                plain_n += st.get('n_files') or 0
                plain_bytes += st.get('total_bytes') or 0
                continue
            rows.append((task_id, name, st.get('n_files'), st.get('total_bytes'),
                         st.get('checksum')))
        if plain_n:
            rows.append((task_id, '*', plain_n, plain_bytes, None))
        self.conn.executemany(
            'INSERT INTO output_stat(task_id, name, n_files, total_bytes, checksum) '
            'VALUES (?, ?, ?, ?, ?)', rows)

    def get_output_stats(self, task):
        rows = self.conn.execute(
            'SELECT s.name, s.n_files, s.total_bytes, s.checksum FROM output_stat s '
            'JOIN task t ON s.task_id = t.id WHERE t.key = ?', (task.key,))
        return {name: {'n_files': n, 'total_bytes': b, 'checksum': c}
                for name, n, b, c in rows}

    def get_rule_output_bytes(self, rules):
        wanted = set(rules)
        rows = self.conn.execute(
            'SELECT r.name, SUM(s.total_bytes) FROM output_stat s '
            'JOIN task t ON s.task_id = t.id JOIN rule r ON t.rule_id = r.id '
            'GROUP BY r.name')
        return {name: total or 0 for name, total in rows if name in wanted}

    def update_tasks(self, tasks, status, exception=''):
        self._commit_updates(tasks, status, exception, self.current_run_seq())

    @retry_lock_commit
    def _commit_updates(self, tasks, status, exception, run_seq, resources=None,
                        output_stats=None, execution=None):
        # One EXCLUSIVE transaction for the lot (bulk state changes:
        # set-state, migration adoption).
        for task in tasks:
            self._upsert_task(task, status, exception, run_seq, resources)
            if output_stats is not None:
                self._write_output_stats(task.key, output_stats)
            if resources is not None:
                # Measured, so an execution: one history row (item 3).
                self._write_history(task.key, status, run_seq, resources, output_stats,
                                    exception, execution)

    def mark_pending(self, tasks):
        """set-state --pending: PENDING, keeping each record's run_seq. A
        fresh run_seq would make the planner's cross-run backstop rerun
        up-to-date downstream tasks, though nothing upstream ran
        (ultrareview 2026-11-21)."""
        self._commit_updates(tasks, TASK_STATUS_PENDING, '', None)

    @retry_lock_commit
    def delete_tasks(self, tasks):
        keys = [task.key for task in tasks]
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            for table in ('task_run', 'output_stat'):
                self.conn.execute(
                    f'DELETE FROM {table} WHERE task_id IN '
                    f'(SELECT id FROM task WHERE key IN ({placeholders}))', chunk)
            self.conn.execute(f'DELETE FROM task WHERE key IN ({placeholders})', chunk)

    def _upsert_task(self, task, status, exception='', run_seq=None, resources=None):
        # The uses/io ids were computed and interned once per rule at
        # ensure_rules time — no per-task hashing or text writes (the old
        # per-task compute_uses_hash was 1e6 AST renders on a big run).
        rule_id, run_code_id, uses_code_id, io_code_id = self.rule_ids[task.rule.name]
        res = resources or {}
        if getattr(task, 'legacy_key', None):
            # Re-key a record stored under the task's pre-canonical key (M19).
            self.conn.execute(
                'UPDATE task SET key = ? WHERE key = ? AND NOT EXISTS '
                '(SELECT 1 FROM task WHERE key = ?)',
                (task.key, task.legacy_key, task.key))
        from ..core.task import paths_digest
        digest = paths_digest(task)
        # An execution replaces all four resource columns verbatim, NULLs
        # included, so a fresh wall_s is never left paired with a peak RSS
        # from an earlier run. A bulk state change (resources=None) measured
        # nothing and leaves them alone: `set-state --pending` does not
        # un-measure what actually ran (design_docs/designs/resource_capture.md).
        resource_update = (
            '    wall_s = excluded.wall_s, '
            '    cpu_s = excluded.cpu_s, '
            '    max_rss_bytes = excluded.max_rss_bytes, '
            '    rss_method = excluded.rss_method, '
        ) if resources is not None else ''
        self.conn.execute(
            'INSERT INTO task(key, rule_id, run_code_id, uses_code_id, io_code_id, '
            '                 run_seq, last_run_timestamp, last_run_status, exception, '
            '                 wall_s, cpu_s, max_rss_bytes, rss_method, paths_digest) '
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?) "
            'ON CONFLICT(key) DO UPDATE SET '
            '    run_code_id = excluded.run_code_id, '
            '    uses_code_id = excluded.uses_code_id, '
            '    io_code_id = excluded.io_code_id, '
            # NULL (mark_pending): keep the stored run_seq.
            '    run_seq = COALESCE(excluded.run_seq, run_seq), '
            "    last_run_timestamp = datetime('now'), "
            f'{resource_update}'
            '    paths_digest = excluded.paths_digest, '
            '    last_run_status = excluded.last_run_status, '
            '    exception = excluded.exception',
            (
                task.key,
                rule_id,
                run_code_id,
                uses_code_id,
                io_code_id,
                run_seq,
                status,
                exception,
                res.get('wall_s'),
                res.get('cpu_s'),
                res.get('max_rss_bytes'),
                res.get('rss_method'),
                digest,
            ),
        )
