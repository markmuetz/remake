"""SQLite metadata backend.

Schema carried over from remake2 with one addition: a uses_hash column on
task. No migration support pre-release: if the schema changes, delete
.remake/remake.db and rerun.
"""
import json
import random
import sqlite3
from pathlib import Path
from time import perf_counter, sleep

from loguru import logger

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
    PRIMARY KEY (id),
    FOREIGN KEY(inputs_code_id) REFERENCES code (id),
    FOREIGN KEY(outputs_code_id) REFERENCES code (id),
    FOREIGN KEY(run_code_id) REFERENCES code (id)
);

CREATE TABLE task (
    id INTEGER NOT NULL,
    key VARCHAR(40) NOT NULL,
    rule_id INTEGER NOT NULL,
    run_code_id INTEGER,
    uses_hash TEXT,
    last_run_timestamp TIMESTAMP,
    last_run_status INTEGER,
    exception TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY(rule_id) REFERENCES rule (id),
    FOREIGN KEY(run_code_id) REFERENCES code (id)
);

CREATE UNIQUE INDEX task_key_index ON task(key);
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
        self.conn.isolation_level = 'EXCLUSIVE'
        self.rule_ids = {}  # rule name -> (rule_id, run_code_id)

    def _insert_code(self, code):
        cur = self.conn.execute('INSERT INTO code(code) VALUES (?)', (code,))
        return cur.lastrowid

    @retry_lock_commit
    def _ensure_rule(self, rule):
        row = self.conn.execute(
            'SELECT id, inputs_code_id, outputs_code_id, run_code_id FROM rule WHERE name = ?',
            (rule.name,),
        ).fetchone()
        source = rule.source
        if row is None:
            code_ids = {part: self._insert_code(source[part]) for part in source}
            cur = self.conn.execute(
                'INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id) '
                'VALUES (?, ?, ?, ?)',
                (rule.name, code_ids['inputs'], code_ids['outputs'], code_ids['run']),
            )
            self.rule_ids[rule.name] = (cur.lastrowid, code_ids['run'])
            return True, False

        rule_id, *code_id_list = row
        code_ids = dict(zip(['inputs', 'outputs', 'run'], code_id_list))
        changed = False
        for part in ['inputs', 'outputs', 'run']:
            (stored,) = self.conn.execute(
                'SELECT code FROM code WHERE id = ?', (code_ids[part],)
            ).fetchone()
            if not self.code_comparer(stored, source[part]):
                code_ids[part] = self._insert_code(source[part])
                changed = True
        if changed:
            logger.trace('rule {}: stored code changed', rule.name)
            self.conn.execute(
                'UPDATE rule SET inputs_code_id = ?, outputs_code_id = ?, run_code_id = ? '
                'WHERE id = ?',
                (code_ids['inputs'], code_ids['outputs'], code_ids['run'], rule_id),
            )
        self.rule_ids[rule.name] = (rule_id, code_ids['run'])
        return row is None, changed

    def ensure_rules(self, rules):
        ninserted = nchanged = 0
        for rule in rules:
            inserted, changed = self._ensure_rule(rule)
            ninserted += inserted
            nchanged += changed
        logger.debug(
            'ensured {} rule(s): {} new, {} with changed code',
            len(rules), ninserted, nchanged,
        )

    # Below SQLite's historical SQLITE_MAX_VARIABLE_NUMBER default of 999.
    SELECT_CHUNK = 900

    def get_tasks_status(self, tasks):
        start = perf_counter()
        records = {}
        keys = [task.key for task in tasks]
        nchunks = (len(keys) + self.SELECT_CHUNK - 1) // self.SELECT_CHUNK
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            rows = self.conn.execute(
                'SELECT task.key, task.last_run_status, task.last_run_timestamp, '
                '       task.uses_hash, task.exception, code.code '
                'FROM task LEFT JOIN code ON task.run_code_id = code.id '
                f'WHERE task.key IN ({placeholders})',
                chunk,
            ).fetchall()
            for key, status, timestamp, stored_uses_hash, exception, run_code in rows:
                records[key] = TaskRecord(
                    key=key,
                    status=status,
                    timestamp=timestamp,
                    run_code=run_code or '',
                    uses_hash=stored_uses_hash or '',
                    exception=exception or '',
                )
        logger.debug(
            'queried status of {} task(s) in {} chunk(s), {} found, in {:.3f}s',
            len(keys), nchunks, len(records), perf_counter() - start,
        )
        return records

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
        logger.debug(
            f'Ingested {len(pending)} sidecar result(s) in '
            f'{perf_counter() - start:.3f}s'
        )
        return len(pending)

    @retry_lock_commit
    def _ingest_records(self, pending):
        for rule, key, payload, _ in pending:
            rule_id, run_code_id = self.rule_ids[rule.name]
            self.conn.execute(
                'INSERT INTO task(key, rule_id, run_code_id, uses_hash, '
                '                 last_run_timestamp, last_run_status, exception) '
                'VALUES (?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(key) DO UPDATE SET '
                '    run_code_id = excluded.run_code_id, '
                '    uses_hash = excluded.uses_hash, '
                '    last_run_timestamp = excluded.last_run_timestamp, '
                '    last_run_status = excluded.last_run_status, '
                '    exception = excluded.exception',
                (
                    key,
                    rule_id,
                    run_code_id,
                    payload.get('uses_hash', ''),
                    payload.get('timestamp'),
                    payload['status'],
                    payload.get('exception', ''),
                ),
            )

    @retry_lock_commit
    def update_task(self, task, status, exception=''):
        self._upsert_task(task, status, exception)

    @retry_lock_commit
    def update_tasks(self, tasks, status, exception=''):
        # One EXCLUSIVE transaction for the lot (bulk state changes:
        # set-state, migration adoption).
        for task in tasks:
            self._upsert_task(task, status, exception)

    @retry_lock_commit
    def delete_tasks(self, tasks):
        keys = [task.key for task in tasks]
        for i in range(0, len(keys), self.SELECT_CHUNK):
            chunk = keys[i:i + self.SELECT_CHUNK]
            placeholders = ','.join('?' * len(chunk))
            self.conn.execute(f'DELETE FROM task WHERE key IN ({placeholders})', chunk)

    def _upsert_task(self, task, status, exception=''):
        rule_id, run_code_id = self.rule_ids[task.rule.name]
        self.conn.execute(
            'INSERT INTO task(key, rule_id, run_code_id, uses_hash, '
            '                 last_run_timestamp, last_run_status, exception) '
            "VALUES (?, ?, ?, ?, datetime('now'), ?, ?) "
            'ON CONFLICT(key) DO UPDATE SET '
            '    run_code_id = excluded.run_code_id, '
            '    uses_hash = excluded.uses_hash, '
            "    last_run_timestamp = datetime('now'), "
            '    last_run_status = excluded.last_run_status, '
            '    exception = excluded.exception',
            (
                task.key,
                rule_id,
                run_code_id,
                compute_uses_hash(task.rule.uses),
                status,
                exception,
            ),
        )
