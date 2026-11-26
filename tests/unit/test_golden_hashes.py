"""Golden hashes: what remake stores for a fixed corpus of rules must not move.

Task keys, the stored `uses` and `io` renderings (what `ensure_rules` interns
as code rows: `rule_uses_hash` and `io_hash`) and each task's `paths_digest`
are written to the DB, so any change to how they are computed makes every
existing pipeline rerun on upgrade. This test builds a corpus of rules (several
matrix forms, `uses=`, callable specs, `Dir`, `Ensure`, list values), computes
those values with the current code and compares them with
`golden_hashes.json`. A mismatch names the rule, the task (where per-task)
and the field.

A change here must be a deliberate act: if the new values are intended (and
the release's upgrade story, see design_docs/releasing.md, covers the rerun),
regenerate the file and commit it with the code change:

    REMAKE_UPDATE_GOLDEN=1 uv run pytest tests/unit/test_golden_hashes.py

Editing the corpus below (including the source text of its functions, which
the renderings embed) also changes the golden values, and needs the same
regeneration.
"""
import json
import os
from pathlib import Path

import pytest

from remake import Dir, Ensure, Remake, Sqlite3Backend, rule
from remake.core.scope import io_hash, rule_uses_hash
from remake.core.task import paths_digest

GOLDEN = Path(__file__).with_name('golden_hashes.json')

SCALE = 3
FACTOR = {'a': 1, 'b': 2}


def helper(x):
    """A helper tracked through uses=."""
    return x * SCALE


def list_spec():
    """A callable inputs spec whose value is a list of Paths."""
    return {'files': [Path('data/g/{}.txt'.format(i)) for i in range(2)]}


def dict_spec():
    """A callable inputs spec returning a plain mapping."""
    return {'x': 'data/g/dict_spec.txt'}


@rule(outputs={'o': 'out/g_cart_{a}_{b}.txt'}, matrix={'a': [1, 2], 'b': ['x', 'y']})
def g_cartesian(outputs, a, b):
    """Cartesian matrix of two axes."""


@rule(outputs={'o': 'out/g_tuple_{a}_{b}.txt'}, matrix={('a', 'b'): [(1, 'x'), (2, 'y')]})
def g_tuple_key(outputs, a, b):
    """A tuple-key matrix of pre-filtered combinations."""


@rule(outputs={'o': 'out/g_rows_{a}_{b}.txt'}, matrix=[{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'z'}])
def g_row_list(outputs, a, b):
    """A non-cartesian list[dict] matrix."""


@rule(inputs={'i': 'in/g_{n}.txt'}, outputs={'o': 'out/g_float_{n}_{f}.txt'},
      matrix={'n': [1, 2], 'f': [0.5, 1.5]})
def g_inputs_outputs(inputs, outputs, n, f):
    """Plain inputs and outputs with float and int matrix values."""


@rule(outputs={'o': 'out/g_uses_{k}.txt'}, matrix={'k': ['a', 'b']},
      uses={'SCALE': SCALE, 'helper': helper, 'FACTOR': FACTOR})
def g_uses(outputs, k):
    """uses= with a constant, a helper function and a dict."""


@rule(inputs=list_spec, outputs={'o': 'out/g_list_spec.txt'})
def g_callable_list(inputs, outputs):
    """A callable spec returning a list of Paths."""


@rule(inputs=dict_spec, outputs={'o': 'out/g_dict_spec.txt'})
def g_callable_dict(inputs, outputs):
    """A callable spec returning a mapping."""


@rule(outputs={'d': Dir('out/g_dir_{k}')}, matrix={'k': [1, 2]})
def g_dir(outputs, k):
    """A Dir output."""


@rule(outputs={'o': Ensure('out/g_ensure_{k}.txt', min_size=4, nonempty=True, checksum=True)},
      matrix={'k': [1, 2]})
def g_ensure(outputs, k):
    """Ensure on a file output, with options."""


@rule(inputs={'both': ['in/g_{k}_a.txt', 'in/g_{k}_b.txt']},
      outputs={'files': ['out/g_lv_{k}_a.txt', 'out/g_lv_{k}_b.txt'], 'one': 'out/g_lv_{k}.txt'},
      matrix={'k': [1, 2]})
def g_list_values(inputs, outputs, k):
    """List-valued inputs and outputs next to a scalar output."""


@rule(inputs={'x': 'in/g_noinput.txt'}, outputs={'o': 'out/g_none.txt'})
def g_no_matrix(inputs, outputs):
    """No matrix: one task with empty kwargs."""


RULES = [g_cartesian, g_tuple_key, g_row_list, g_inputs_outputs, g_uses,
         g_callable_list, g_callable_dict, g_dir, g_ensure, g_list_values, g_no_matrix]


def compute():
    """The golden structure for the corpus, computed with the current code.

    {rule name: {'uses': ..., 'io': ..., 'tasks': {task key: {'kwargs': ...,
    'paths_digest': ...}}}}
    """
    rmk = Remake(rules=RULES, metadata=Sqlite3Backend(':memory:'))
    try:
        out = {}
        for r in rmk.rules:
            tasks = {}
            for t in rmk.tasks(f'rule == {r.name!r}'):
                tasks[t.key] = {'kwargs': repr(dict(sorted(t.kwargs.items()))),
                                'paths_digest': paths_digest(t)}
            out[r.name] = {'uses': rule_uses_hash(r), 'io': io_hash(r), 'tasks': tasks}
        return out
    finally:
        rmk.metadata.close()


def differences(golden, current):
    """Human-readable lines naming each rule, task and field that differs."""
    lines = []
    for name in sorted(set(golden) | set(current)):
        if name not in current:
            lines.append(f'rule {name}: in the golden file, not in the corpus')
            continue
        if name not in golden:
            lines.append(f'rule {name}: in the corpus, not in the golden file')
            continue
        g, c = golden[name], current[name]
        for field in ('uses', 'io'):
            if g[field] != c[field]:
                lines.append(f'rule {name}, field {field}:\n  golden:  {g[field]!r}\n'
                             f'  current: {c[field]!r}')
        for key in sorted(set(g['tasks']) | set(c['tasks'])):
            gt, ct = g['tasks'].get(key), c['tasks'].get(key)
            if gt is None or ct is None:
                which = ct or gt
                lines.append(f'rule {name}, task {which["kwargs"]} (key {key[:8]}): task key '
                             f'{"is new" if gt is None else "is gone"} (the key changed)')
            elif gt['paths_digest'] != ct['paths_digest']:
                lines.append(f'rule {name}, task {gt["kwargs"]}, field paths_digest:\n'
                             f'  golden:  {gt["paths_digest"]}\n  current: {ct["paths_digest"]}')
    return lines


def test_golden_hashes():
    """Task keys, uses/io renderings and paths_digest match the golden file."""
    current = compute()
    if os.environ.get('REMAKE_UPDATE_GOLDEN') == '1':
        GOLDEN.write_text(json.dumps(current, indent=2, sort_keys=True) + '\n')
        pytest.skip(f'regenerated {GOLDEN.name}')
    golden = json.loads(GOLDEN.read_text())
    lines = differences(golden, current)
    assert not lines, (
        'stored hashes changed: existing pipelines would rerun on upgrade. If '
        'intended, regenerate with REMAKE_UPDATE_GOLDEN=1 (see this module\'s '
        'docstring).\n' + '\n'.join(lines))
