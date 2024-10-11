from pathlib import Path
import sqlite3
import random
from time import sleep

from loguru import logger

from .metadata_manager import MetadataManager

DBLOC = '.remake/remake.db'

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
	key VARCHAR(40) NOT NULL, --indexed.
	rule_id INTEGER NOT NULL,
	code_id INTEGER,
    last_run_timestamp TIMESTAMP,
    last_run_status INTEGER,
    exception TEXT,
	PRIMARY KEY (id),
	FOREIGN KEY(rule_id) REFERENCES rule (id),
	FOREIGN KEY(code_id) REFERENCES code (id)
);


-- This speeds things up by an order of magnitude.
-- When using :memory:
CREATE INDEX table_key_index
ON task(key);
"""


def retry(fn):
    def inner(self, conn, *args, **kwargs):
        nattempts = 1
        logger.trace(f'>{fn.__name__}')
        while True:
            try:
                with conn:
                    ret = fn(self, conn, *args, **kwargs)
                    logger.trace(f'<{fn.__name__}')
                return ret
            except sqlite3.OperationalError as oe:
                logger.debug(f'    OperationalError: {oe}')
            logger.trace(f'    retry {nattempts}')
            nattempts += 1
            sleep(2**nattempts * random.random())

    return inner


def retry_lock_commit(fn):
    def inner(self, conn, *args, **kwargs):
        nattempts = 1
        logger.trace(f'>{fn.__name__}')
        while True:
            try:
                with conn:
                    conn.execute('BEGIN EXCLUSIVE')

                    logger.trace('  locked')
                    ret = fn(self, conn, *args, **kwargs)
                    conn.commit()
                    logger.trace('  commited')
                    logger.trace(f'<{fn.__name__}')
                return ret
            except sqlite3.OperationalError as oe:
                logger.debug(f'    OperationalError: {oe}')
            logger.trace(f'    retry {nattempts}')
            nattempts += 1
            sleep(2**nattempts * random.random())

    return inner


class Sqlite3MetadataManager(MetadataManager):
    def __init__(self):
        super().__init__()

        create_db = not Path(DBLOC).exists()
        if create_db:
            logger.info(f'Creating sqlite3 database: {DBLOC}')
            Path('.remake').mkdir(exist_ok=True)
        self.conn = sqlite3.connect(DBLOC, detect_types=sqlite3.PARSE_DECLTYPES)
        if create_db:
            self.conn.executescript(SQL_SCHEMA)
        # This works and is blazingly fast, but at the cost that you can no longer write to db :(
        # self.conn_disk = sqlite3.connect(DBLOC)
        # self.conn = sqlite3.connect(':memory:')
        # self.conn_disk.backup(self.conn)
        self.conn.isolation_level = 'EXCLUSIVE'

        self.rule_map = {}

    @retry
    def _select_db_rule(self, conn, rule):
        return conn.execute('SELECT * FROM rule WHERE name = ?', (rule.__name__,)).fetchone()

    @retry_lock_commit
    def _create_db_rule(self, conn, rule):
        code_ids = {}
        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            code = rule.source[req_method]
            conn.execute('INSERT INTO code(code) VALUES (?)', (code,))
            code_ids[req_method] = conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]

        conn.execute(
            'INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id) VALUES (?, ?, ?, ?)',
            (
                rule.__name__,
                code_ids['rule_inputs'],
                code_ids['rule_outputs'],
                code_ids['rule_run'],
            ),
        )

    @retry
    def _select_code_from_rule(self, conn, dbname, db_rule):
        db_code = conn.execute(
            f'SELECT code.id, code.code FROM rule INNER JOIN code ON rule.{dbname} = code.id WHERE rule.id = ?',
            (db_rule[0],),
        ).fetchone()
        return db_code

    @retry_lock_commit
    def _insert_code(self, conn, code):
        conn.execute('INSERT INTO code(code) VALUES (?)', (code,))
        return conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]

    @retry_lock_commit
    def _update_rule_code(self, conn, db_rule, code_ids):
        conn.execute(
            'UPDATE rule SET inputs_code_id = ?, outputs_code_id = ?, run_code_id = ? WHERE id = ?',
            (code_ids['rule_inputs'], code_ids['rule_outputs'], code_ids['rule_run'], db_rule[0]),
        )

    def get_or_create_rule_metadata(self, rule):
        db_rule = self._select_db_rule(self.conn, rule)
        if not db_rule:
            logger.debug(f'creating {rule}')
            self._create_db_rule(self.conn, rule)
            db_rule = self._select_db_rule(self.conn, rule)
        else:
            logger.debug(f'got {db_rule}')
            code_ids = {}
            code_differences = False
            for req_method, dbname, code_idx in [
                ('rule_inputs', 'inputs_code_id', 2),
                ('rule_outputs', 'outputs_code_id', 3),
                ('rule_run', 'run_code_id', 4),
            ]:
                db_code = self._select_code_from_rule(self.conn, dbname, db_rule)
                if not self.code_comparer(db_code[1], rule.source[req_method]):
                    logger.trace(f'code different for {req_method}')
                    code = rule.source[req_method]
                    max_code_id = self._insert_code(self.conn, code)
                    code_ids[req_method] = max_code_id
                    code_differences = True
                else:
                    code_ids[req_method] = db_rule[code_idx]

            if code_differences:
                self._update_rule_code(self.conn, db_rule, code_ids)
                db_rule = self._select_db_rule(self.conn, rule)

        self.rule_map[rule] = db_rule

    @retry
    def _select_task_last_run_code(self, conn, task):
        logger.trace(f'_select_task_last_run_code: {task.key()}')
        # return conn.execute('SELECT task.last_run_timestamp, task.last_run_status, code.code FROM task INNER JOIN code ON task.code_id = code.id WHERE task.key = ?', (task.key(), )).fetchone()
        db_ret = conn.execute(
            'SELECT last_run_timestamp, last_run_status, exception, code_id FROM task WHERE key = ?',
            (task.key(),),
        ).fetchone()
        if db_ret:
            last_run_timestamp, last_run_status, last_run_exception, last_run_code_id = db_ret
            logger.trace(db_ret)
            if last_run_code_id:
                (code,) = conn.execute(
                    'SELECT code.code FROM task INNER JOIN code ON task.code_id = code.id WHERE task.key = ?',
                    (task.key(),),
                ).fetchone()
                return (last_run_timestamp, last_run_status, last_run_exception, code)
            else:
                return (last_run_timestamp, last_run_status, last_run_exception, '')
        else:
            return db_ret

    @retry_lock_commit
    def _insert_tasks(self, conn, tasks):
        task_data = [(t.key(), self.rule_map[t.rule][0], 0) for t in tasks]
        logger.trace(f'inserting {len(tasks)} tasks')
        conn.executemany(
            'INSERT INTO task(key, rule_id, last_run_status) VALUES (?, ?, ?)', task_data
        )
        logger.trace(f'inserted {len(tasks)} tasks')

    def get_or_create_tasks_metadata(self, tasks):
        tasks_to_insert = []

        # This block of code is read only, and this speeds up access massively.
        mem_conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.backup(mem_conn)
        # mem_conn = self.conn

        for task in tasks:
            db_task_last_run_code = self._select_task_last_run_code(mem_conn, task)
            logger.trace(db_task_last_run_code)
            if not db_task_last_run_code:
                db_exists = False
            else:
                db_exists = True
                task.last_run_timestamp = db_task_last_run_code[0]
                task.last_run_status = db_task_last_run_code[1]
                task.last_run_exception = db_task_last_run_code[2]
                task.last_run_code = db_task_last_run_code[3]

            if not db_exists:
                tasks_to_insert.append(task)
        mem_conn.close()

        self._insert_tasks(self.conn, tasks_to_insert)

    @retry_lock_commit
    def _update_task_metadata(self, conn, task, code_id, exception=''):
        conn.execute(
            f'UPDATE task SET code_id = (?), last_run_timestamp = datetime(\'now\'), last_run_status = ?, exception = ? WHERE key = ?',
            (code_id, task.last_run_status, exception, task.key()),
        )

    def update_task_metadata(self, task, exception=''):
        code_id = self.rule_map[task.rule][4]
        self._update_task_metadata(self.conn, task, code_id, exception)
