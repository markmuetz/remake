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
-- With commented out:
-- %timeit -n1 -r1 %run ex1.py
-- 40s
-- %timeit -n1 -r1 rmk.run()
-- 21s
-- %timeit -n1 -r1 %run -i ex1.py
-- 42s
-- With uncommented:
-- %timeit -n1 -r1 %run ex1.py
-- 985ms
-- %timeit -n1 -r1 rmk.run()
-- 594ms
-- %timeit -n1 -r1 %run -i ex1.py
-- 3.85s
CREATE INDEX table_key_index
ON task(key);
"""


class Sqlite3MetadataManager:
    def __init__(self):
        self.conn = sqlite3.connect(dbloc)
        # This works and is blazingly fast, but at the cost that you can no longer write to db :(
        # self.conn_disk = sqlite3.connect(dbloc)
        # self.conn = sqlite3.connect(':memory:')
        # self.conn_disk.backup(self.conn)
        self.conn.isolation_level = 'EXCLUSIVE'

        self.rule_map = {}
        self.code_comparer = CodeComparer()

    def create_db(self):
        self.conn.executescript(sql_schema)

    def get_or_create_rule_metadata(self, rule):
        with self.conn:
            db_rule = self.conn.execute('SELECT * FROM rule WHERE name = ?', (rule.__name__, )).fetchone()
            if not db_rule:
                logger.trace(f'creating {rule}')
                self.conn.execute('BEGIN EXCLUSIVE')
                code_ids = {}
                for req_method in ['rule_inputs', 'rule_outputs', 'rule_run']:
                    code = rule.source[req_method]
                    self.conn.execute('INSERT INTO code(code) VALUES (?)', (code, ))
                    code_ids[req_method] = self.conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]

                self.conn.execute('INSERT INTO rule(name, inputs_code_id, outputs_code_id, run_code_id) VALUES (?, ?, ?, ?)',
                                  (rule.__name__, code_ids['rule_inputs'],
                                   code_ids['rule_outputs'], code_ids['rule_run']))
                self.conn.commit()
                db_rule = self.conn.execute('SELECT * FROM rule WHERE name = ?', (rule.__name__, )).fetchone()
            else:
                logger.trace(f'got {db_rule}')
                code_ids = {}
                for req_method, dbname, code_idx in [
                    ('rule_inputs', 'inputs_code_id', 2),
                    ('rule_outputs', 'outputs_code_id', 3),
                    ('rule_run', 'run_code_id', 4)]:
                    db_code = self.conn.execute(
                        f'SELECT code.id, code.code FROM rule INNER JOIN code ON rule.{dbname} = code.id WHERE rule.id = ?',
                        (db_rule[0], )
                    ).fetchone()
                    if not self.code_comparer(db_code[1], rule.source[req_method]):
                        logger.trace(f'code different for {req_method}')
                        self.conn.execute('BEGIN EXCLUSIVE')
                        code = rule.source[req_method]
                        self.conn.execute('INSERT INTO code(code) VALUES (?)', (code, ))
                        code_ids[req_method] = self.conn.execute('SELECT MAX(id) FROM code;').fetchone()[0]
                        self.conn.commit()
                    else:
                        code_ids[req_method] = db_rule[code_idx]
                self.conn.execute('BEGIN EXCLUSIVE')
                self.conn.execute('UPDATE rule SET inputs_code_id = ?, outputs_code_id = ?, run_code_id = ? WHERE id = ?',
                                  (code_ids['rule_inputs'], code_ids['rule_outputs'], code_ids['rule_run'], db_rule[0]))
                self.conn.commit()
                db_rule = self.conn.execute('SELECT * FROM rule WHERE name = ?', (rule.__name__, )).fetchone()


            self.rule_map[rule] = db_rule

    def insert_tasks(self, tasks):
        task_data = [(t.key(), self.rule_map[t.rule][0], True) for t in tasks]
        with self.conn:
            logger.debug(f'Inserting {len(tasks)} tasks')
            self.conn.execute('BEGIN EXCLUSIVE')
            self.conn.executemany('INSERT INTO task(key, rule_id, requires_rerun) VALUES (?, ?, ?)', task_data)
            self.conn.commit()
            logger.debug(f'Inserted {len(tasks)} tasks')

    def tasks_requires_rerun(self, tasks):
        # tasks_to_insert = []
        # for task in tqdm(self.topo_tasks):
        #     exists, requires_rerun = self.metadata_manager.task_requires_rerun(mem_conn, task)
        #     if not exists:
        #         tasks_to_insert.append(task)
        #     if requires_rerun:
        #         task.requires_rerun = True
        #     else:
        #         task.requires_rerun = False
        # self.metadata_manager.insert_tasks(tasks_to_insert)

        tasks_to_insert = []

        # This block of code is read only, and this speeds up access massively.
        # mem_conn = sqlite3.connect(':memory:')
        # self.conn.backup(mem_conn)
        mem_conn = self.conn

        with mem_conn:
        # with self.conn:
            for task in tasks:
                db_code = mem_conn.execute('SELECT task.id, code.code FROM task INNER JOIN code ON task.code_id = code.id WHERE key = ?', (task.key(), )).fetchone()
                # db_task = mem_conn.execute('SELECT * FROM task WHERE key = ?', (task.key(), )).fetchone()
                if not db_code:
                    requires_rerun = True
                    exists = False
                else:
                    exists = True
                    code = db_code[1]
                    # print(code == task.rule.source['rule_run'])
                    requires_rerun = not self.code_comparer(code, task.rule.source['rule_run'])

                if not exists:
                    tasks_to_insert.append(task)
                if requires_rerun:
                    task.requires_rerun = True
                else:
                    task.requires_rerun = False

        self.insert_tasks(tasks_to_insert)

    def update_task_metadata(self, task):
        with self.conn:
            code_id = self.rule_map[task.rule][4]
            self.conn.execute('BEGIN EXCLUSIVE')
            self.conn.execute(f'UPDATE task SET code_id = (?), requires_rerun = ? WHERE key = ?', (code_id, False, task.key()))
            self.conn.commit()

    def update_tasks(self, tasks, requires_rerun):
        with self.conn:
            self.conn.execute('BEGIN EXCLUSIVE')
            for task in tasks:
                self.conn.execute(f'UPDATE task SET requires_rerun = ? WHERE key = ?', (requires_rerun, task.key()))
            self.conn.commit()


