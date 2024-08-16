from pathlib import Path
import sqlite3

from loguru import logger

from .code_compare import CodeComparer

dbloc = '.remake/remake2.db'
# dbloc = '/work/scratch-nopw2/mmuetz/remake2.db'

sql_schema = """
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
        requires_rerun BOOL NOT NULL,
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
                logger.trace(f'    OperationalError: {oe}')
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
                logger.trace(f'    OperationalError: {oe}')
            logger.trace(f'    retry {nattempts}')
            nattempts += 1
            sleep(2**nattempts * random.random())
    return inner


class Sqlite3MetadataManager:
    def __init__(self):
        create_db = not Path(dbloc).exists()
        self.conn = sqlite3.connect(dbloc)
        if create_db:
            self.create_db()
        # This works and is blazingly fast, but at the cost that you can no longer write to db :(
        # self.conn_disk = sqlite3.connect(dbloc)
        # self.conn = sqlite3.connect(':memory:')
        # self.conn_disk.backup(self.conn)
        self.conn.isolation_level = 'EXCLUSIVE'

        self.rule_map = {}
        self.code_comparer = CodeComparer()

    def create_db(self):
        self.conn.executescript(sql_schema)

    @retry
    def _select_db_rule(self, conn, rule):
        return conn.execute('SELECT * FROM rule WHERE name = ?', (rule.__name__, )).fetchone()

    @retry_lock_commit
    def _create_db_rule(self, conn, rule):
        code_ids = {}
        for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
            code = rule.source[req_method]
            conn.execute('INSERT INTO code(code) VALUES (?)', (code, ))
            code_ids[req_method] = conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]

        conn.execute('INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id) VALUES (?, ?, ?, ?)',
                     (rule.__name__, code_ids['rule_inputs'],
                      code_ids['rule_outputs'], code_ids['rule_run']))

    @retry
    def _select_code_from_rule(self, conn, dbname, db_rule):
        db_code = conn.execute(
            f'SELECT code.id, code.code FROM rule INNER JOIN code ON rule.{dbname} = code.id WHERE rule.id = ?',
            (db_rule[0], )
        ).fetchone()
        return db_code

    @retry_lock_commit
    def _insert_code(self, conn, code):
        conn.execute('INSERT INTO code(code) VALUES (?)', (code, ))
        return conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]

    @retry_lock_commit
    def _update_rule_code(self, conn, db_rule, code_ids):
        conn.execute('UPDATE rule SET inputs_code_id = ?, outputs_code_id = ?, run_code_id = ? WHERE id = ?',
                     (code_ids['rule_inputs'], code_ids['rule_outputs'], code_ids['rule_run'], db_rule[0]))

    def get_or_create_rule_metadata(self, rule):
        db_rule = self._select_db_rule(self.conn, rule)
        if not db_rule:
            logger.trace(f'creating {rule}')
            self._create_db_rule(self.conn, rule)
            db_rule = self._select_db_rule(self.conn, rule)
        else:
            logger.trace(f'got {db_rule}')
            code_ids = {}
            for req_method, dbname, code_idx in [
                ('rule_inputs', 'inputs_code_id', 2),
                ('rule_outputs', 'outputs_code_id', 3),
                ('rule_run', 'run_code_id', 4)]:
                db_code = self._select_code_from_rule(self.conn, dbname, db_rule)
                if not self.code_comparer(db_code[1], rule.source[req_method]):
                    logger.trace(f'code different for {req_method}')
                    code = rule.source[req_method]
                    max_code_id = self._insert_code(self.conn, code)
                    code_ids[req_method] = max_code_id
                else:
                    code_ids[req_method] = db_rule[code_idx]

            self._update_rule_code(self.conn, db_rule, code_ids)
            db_rule = self._select_db_rule(self.conn, rule)

        self.rule_map[rule] = db_rule

    @retry_lock_commit
    def _insert_tasks(self, conn, task_data):
        conn.executemany('INSERT INTO task(key, rule_id, requires_rerun) VALUES (?, ?, ?)', task_data)

    def insert_tasks(self, tasks):
        task_data = [(t.key(), self.rule_map[t.rule][0], True) for t in tasks]
        logger.trace(f'Inserting {len(tasks)} tasks')
        self._insert_tasks(self.conn, task_data)
        logger.trace(f'Inserted {len(tasks)} tasks')

    @retry
    def _select_task_reqs_rerun_code(self, conn, task):
        return conn.execute('SELECT task.requires_rerun, code.code FROM task INNER JOIN code ON task.code_id = code.id WHERE key = ?', (task.key(), )).fetchone()

    def tasks_requires_rerun(self, tasks):
        tasks_to_insert = []

        # This block of code is read only, and this speeds up access massively.
        mem_conn = sqlite3.connect(':memory:')
        self.conn.backup(mem_conn)
        # mem_conn = self.conn

        for task in tasks:
            db_requires_rerun_code = self._select_task_reqs_rerun_code(mem_conn, task)
            if not db_requires_rerun_code:
                requires_rerun = True
                exists = False
            else:
                exists = True
                db_requires_rerun = db_requires_rerun_code[0]
                code = db_requires_rerun_code[1]
                # print(code == task.rule.source['rule_run'])
                requires_rerun = db_requires_rerun or not self.code_comparer(code, task.rule.source['rule_run'])

            if not exists:
                tasks_to_insert.append(task)
            if requires_rerun:
                task.requires_rerun = True
            else:
                task.requires_rerun = False
        mem_conn.close()

        self.insert_tasks(tasks_to_insert)

    @retry_lock_commit
    def _update_task_metadata(self, conn, task, code_id):
        conn.execute(f'UPDATE task SET code_id = (?), requires_rerun = ? WHERE key = ?', (code_id, False, task.key()))

    def update_task_metadata(self, task):
        code_id = self.rule_map[task.rule][4]
        self._update_task_metadata(self.conn, task, code_id)

    @retry_lock_commit
    def _update_tasks(self, conn, tasks, requires_rerun):
        for task in tasks:
            self.conn.execute(f'UPDATE task SET requires_rerun = ? WHERE key = ?', (requires_rerun, task.key()))

    def update_tasks(self, tasks, requires_rerun):
        self._update_tasks(self.conn, tasks, requires_rerun)


