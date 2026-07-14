"""SQLite metadata backend.

Schema carried over from remake2, with the task table reworked so that all
code-derived state (run source, the normalised uses/io strings) lives in the
content-addressed `code` table and task rows carry only integer FKs. Storing
the uses/io text inline per task made real DBs enormous (272 MB for 3341
tasks, 99.8% of it duplicated uses text) and made status queries scale with
task count (design_docs/logs_analysis/README.md). Older DBs are migrated in
place by `_add_missing_columns`.
"""
import json
import random
import sqlite3
from pathlib import Path
from time import perf_counter, sleep

from loguru import logger

from ..core.scope import io_hash as compute_io_hash
from ..core.scope import raw_uses_parts
from ..core.scope import uses_hash as compute_uses_hash
from ..util.code_compare import CodeComparer
from .metadata_manager import MetadataManager, TaskRecord

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


def retry_lock_commit(fn):
    """Run fn in an EXCLUSIVE transaction, retrying with exponential backoff
    on lock contention (concurrent workers/SLURM jobs share the DB)."""

    def inner(self, *args, **kwargs):
        nattempts = 1
        while True:
            try:
                with self.conn:
                    self.conn.execute('BEGIN EXCLUSIVE')
                    ret = fn(self, *args, **kwargs)
                    self.conn.commit()
                return ret
            except sqlite3.OperationalError as oe:
                logger.debug(f'OperationalError: {oe}')
            nattempts += 1
            sleep(2**nattempts * random.random())

    return inner


class Sqlite3Backend(MetadataManager):
    def __init__(self, dbloc='.remake/remake.db'):
        self.dbloc = str(dbloc)
        self.code_comparer = CodeComparer()
        in_memory = self.dbloc == ':memory:'
        create_db = in_memory or not Path(self.dbloc).exists()
        if create_db and not in_memory:
            logger.info(f'Creating sqlite3 database: {self.dbloc}')
            Path(self.dbloc).parent.mkdir(parents=True, exist_ok=True)
        # No detect_types: timestamps are read as plain strings (TaskRecord
        # .timestamp), and the implicit converter is deprecated in 3.12.
        self.conn = sqlite3.connect(self.dbloc)
        if create_db:
            self.conn.executescript(SQL_SCHEMA)
        else:
            self._add_missing_columns()
        self.conn.isolation_level = 'EXCLUSIVE'
        # rule name -> (rule_id, run_code_id, uses_code_id, io_code_id):
        # the rule's row plus this invocation's current interned code ids.
        self.rule_ids = {}
        # Lazily allocated once per process (= per invocation); shared by every
        # task this invocation commits. See the `meta` table comment.
        self._run_seq = None

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

    def _add_missing_columns(self):
        """Lightweight forward-compat for columns added after a DB was first
        created (still no general migration support — see the module docstring).
        A pre-existing record left with io_code_id/run_seq NULL is treated as
        'not yet tracked' by the planner, so upgrading does not force a mass
        rerun."""
        cols = {row[1] for row in self.conn.execute('PRAGMA table_info(task)')}
        if 'run_seq' not in cols:
            logger.info('Adding task.run_seq column to existing DB')
            self.conn.execute('ALTER TABLE task ADD COLUMN run_seq INTEGER')
        if 'uses_code_id' not in cols:
            self._migrate_inline_hashes_to_code_ids(cols)
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
        if 'meta' not in tables:
            logger.info('Adding meta table to existing DB')
            self.conn.execute(
                'CREATE TABLE meta (key TEXT NOT NULL PRIMARY KEY, value INTEGER NOT NULL)')
            self.conn.execute("INSERT INTO meta(key, value) VALUES ('run_seq', 0)")
            self.conn.commit()

    def _migrate_inline_hashes_to_code_ids(self, cols):
        """One-time in-place migration: the old task.uses_hash/io_hash columns
        stored the full normalised uses/io strings inline per row — duplicated
        across every task of a rule (measured at 99.8% of a 272 MB field DB).
        Intern each distinct value into `code` once, point integer FKs at it,
        drop the text columns, and VACUUM to return the space."""
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
        self.conn.commit()
        logger.info('Vacuuming (one-time; reclaims the inline-hash space)')
        self.conn.execute('VACUUM')

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
        uses_code_id = self._intern_code(compute_uses_hash(rule.uses))
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
        keys = [task.key for task in tasks]
        nchunks = (len(keys) + self.SELECT_CHUNK - 1) // self.SELECT_CHUNK
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            rows = self.conn.execute(
                'SELECT key, last_run_status, last_run_timestamp, '
                '       run_code_id, uses_code_id, io_code_id, run_seq, exception '
                f'FROM task WHERE key IN ({placeholders})',
                chunk,
            ).fetchall()
            for (key, status, timestamp, run_code_id, uses_code_id,
                 io_code_id, run_seq, exception) in rows:
                records[key] = TaskRecord(
                    key=key,
                    status=status,
                    timestamp=timestamp,
                    run_code_id=run_code_id,
                    uses_code_id=uses_code_id,
                    exception=exception or '',
                    io_code_id=io_code_id,  # None for pre-upgrade records
                    run_seq=run_seq,  # None for pre-upgrade/never-stamped records
                )
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
                except json.JSONDecodeError:
                    logger.warning(f'Skipping unreadable sidecar: {path}')
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
            self.conn.execute(
                'INSERT INTO task(key, rule_id, run_code_id, uses_code_id, io_code_id, '
                '                 run_seq, last_run_timestamp, last_run_status, exception) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(key) DO UPDATE SET '
                '    run_code_id = excluded.run_code_id, '
                '    uses_code_id = excluded.uses_code_id, '
                '    io_code_id = excluded.io_code_id, '
                '    run_seq = excluded.run_seq, '
                '    last_run_timestamp = excluded.last_run_timestamp, '
                '    last_run_status = excluded.last_run_status, '
                '    exception = excluded.exception',
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
                ),
            )

    def update_task(self, task, status, exception=''):
        # Allocate run_seq (own txn) before opening the upsert's EXCLUSIVE txn.
        self._commit_updates([task], status, exception, self.current_run_seq())

    def update_tasks(self, tasks, status, exception=''):
        self._commit_updates(tasks, status, exception, self.current_run_seq())

    @retry_lock_commit
    def _commit_updates(self, tasks, status, exception, run_seq):
        # One EXCLUSIVE transaction for the lot (bulk state changes:
        # set-state, migration adoption).
        for task in tasks:
            self._upsert_task(task, status, exception, run_seq)

    @retry_lock_commit
    def delete_tasks(self, tasks):
        keys = [task.key for task in tasks]
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            self.conn.execute(f'DELETE FROM task WHERE key IN ({placeholders})', chunk)

    def _upsert_task(self, task, status, exception='', run_seq=None):
        # The uses/io ids were computed and interned once per rule at
        # ensure_rules time — no per-task hashing or text writes (the old
        # per-task compute_uses_hash was 1e6 AST renders on a big run).
        rule_id, run_code_id, uses_code_id, io_code_id = self.rule_ids[task.rule.name]
        self.conn.execute(
            'INSERT INTO task(key, rule_id, run_code_id, uses_code_id, io_code_id, '
            '                 run_seq, last_run_timestamp, last_run_status, exception) '
            "VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?) "
            'ON CONFLICT(key) DO UPDATE SET '
            '    run_code_id = excluded.run_code_id, '
            '    uses_code_id = excluded.uses_code_id, '
            '    io_code_id = excluded.io_code_id, '
            '    run_seq = excluded.run_seq, '
            "    last_run_timestamp = datetime('now'), "
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
            ),
        )
