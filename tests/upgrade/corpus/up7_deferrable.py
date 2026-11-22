"""Upgrade corpus 7: a deferrable (dynamic) matrix."""
import json
from pathlib import Path

from remake import Defer, Remake, deferrable, rule

rmk = Remake()


@rule(outputs={'o': 'data/up7/src_{i}.json'}, matrix={'i': [1, 2, 3]})
def up7_src(outputs, i):
    """Write a source holding a variable number of items."""
    Path(outputs['o']).write_text(json.dumps(list(range(i))))


@deferrable
def up7_matrix():
    """One row per item of every source, once the sources exist."""
    rows = []
    for i in [1, 2, 3]:
        path = Path(f'data/up7/src_{i}.json')
        if not path.exists():
            raise Defer(path)
        rows += [{'i': i, 'item': item} for item in json.loads(path.read_text())]
    return rows


@rule(inputs={'x': 'data/up7/src_{i}.json'}, outputs={'o': 'data/up7/dyn_{i}_{item}.txt'},
      matrix=up7_matrix, depends_on=[up7_src])
def up7_dyn(inputs, outputs, i, item):
    """Write one file per discovered item."""
    Path(outputs['o']).write_text(f'{i} {item}\n')


rmk.rules_from_current_module()
