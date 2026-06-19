# examples/ex11_custom_token.py
#
# Custom output tokens: any object with identity() / is_complete() /
# format() can be an output. Here a DBRow token represents one row in a
# sqlite table — an output that is not a file.
#
# Completion is DB-tracked as usual (remake's own metadata DB); the token's
# is_complete() is the opt-in verification layer. Run with
# `remake run ex9_custom_token.py --check-outputs` after deleting a row
# from data/results.db to see remake detect and recompute it.

import sqlite3
from pathlib import Path

from remake import OutputToken, Remake, rule

rmk = Remake()


class DBRow(OutputToken):
    """One row, identified by key, in a sqlite table."""

    def __init__(self, db, table, key):
        self.db = db
        self.table = table
        self.key = key

    def identity(self):
        return f'db://{self.db}#{self.table}/{self.key}'

    def format(self, **kwargs):
        return DBRow(self.db, self.table, self.key.format(**kwargs))

    def is_complete(self):
        if not Path(self.db).exists():
            return False
        con = sqlite3.connect(self.db)
        try:
            row = con.execute(
                f'SELECT 1 FROM {self.table} WHERE key = ?', (self.key,)
            ).fetchone()
        except sqlite3.OperationalError:  # table does not exist yet
            return False
        finally:
            con.close()
        return row is not None


@rule(
    outputs={'row': DBRow('data/results.db', 'stats', 'n{n}')},
    matrix={'n': [1, 2, 3]},
)
def compute(outputs, n):
    row = outputs['row']
    # DBRow is not path-backed, so remake does not create parent dirs —
    # the rule owns its non-file output completely.
    Path(row.db).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(row.db)
    con.execute(f'CREATE TABLE IF NOT EXISTS {row.table} (key TEXT PRIMARY KEY, value REAL)')
    con.execute(
        f'INSERT OR REPLACE INTO {row.table} VALUES (?, ?)', (row.key, n**2)
    )
    con.commit()
    con.close()


rmk.rules_from_current_module()
