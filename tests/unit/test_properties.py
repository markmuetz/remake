"""Hypothesis property tests for task keys, matrix expansion and digests.

Stored keys and hashes freeze at 1.0 (v0.11.0 item 5), so the invariants the
docs promise are checked over generated inputs, not just hand-picked ones:
keys are deterministic, order-independent and type-sensitive; the matrix
forms agree; `paths_digest` and the io/uses renderings ignore dict order.
"""
import itertools
import math
from hashlib import sha1
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from remake.core.canonical import io_part, uses_hash, uses_parts
from remake.core.dag import expand_rule, resolve_matrix
from remake.core.rule import Rule
from remake.core.task import Task, paths_digest, task_key

PROP = settings(max_examples=150, deadline=None)

NAMES = st.sampled_from(list('abcdefgh')) | st.text(
    alphabet='abcdefghijklmnopqrstuvwxyz_', min_size=1, max_size=6)
SCALARS = st.one_of(
    st.none(), st.booleans(), st.integers(-5, 5),
    st.floats(allow_nan=False, allow_infinity=True),
    st.text(max_size=5),
    st.sampled_from(['1', '1.0', 'True', 'None', '', ' ']))
KWARGS = st.dictionaries(NAMES, SCALARS, max_size=4)
RULE_NAMES = st.text(alphabet='abcxyz_:', min_size=1, max_size=6)


def identity(kwargs):
    """What 'the same kwargs' means: names, value types and values (so
    1, 1.0, True and '1' are four different values)."""
    return {k: (type(v).__name__, repr(v)) for k, v in kwargs.items()}


def reorder(d, seed):
    """The same dict with its insertion order permuted deterministically."""
    items = sorted(d.items())
    n = seed % (len(items) or 1)
    items = items[n:] + items[:n]
    return dict(reversed(items)) if seed % 2 else dict(items)


def make_rule(name='r', matrix=None, inputs=None, outputs=None, uses=None):
    """A Rule built directly (no decorator, so no scope analysis)."""
    def fn():
        pass
    return Rule(fn=fn, matrix=matrix, inputs=inputs, outputs=outputs,
                uses=uses or {}, _name=name)


# --- task keys ---------------------------------------------------------------

@PROP
@given(RULE_NAMES, KWARGS, st.integers(0, 10))
def test_key_is_deterministic_and_ignores_kwargs_order(name, kwargs, seed):
    assert task_key(name, kwargs) == task_key(name, dict(kwargs))
    assert task_key(name, kwargs) == task_key(name, reorder(kwargs, seed))
    assert len(task_key(name, kwargs)) == 40


@PROP
@given(RULE_NAMES, KWARGS, KWARGS)
def test_distinct_kwargs_give_distinct_keys(name, a, b):
    same = identity(a) == identity(b)
    assert (task_key(name, a) == task_key(name, b)) == same


@PROP
@given(RULE_NAMES, RULE_NAMES, KWARGS)
def test_distinct_rule_names_give_distinct_keys(n1, n2, kwargs):
    assert (task_key(n1, kwargs) == task_key(n2, kwargs)) == (n1 == n2)


def test_equal_looking_values_of_different_types_have_different_keys():
    # Current behaviour, asserted so it cannot change silently after 1.0:
    # the key hashes repr(), so 1, 1.0, True and '1' are all different.
    values = [1, 1.0, True, '1', None, 'None', '1.0', 'True', (1,), (1.0,)]
    keys = {task_key('r', {'x': v}) for v in values}
    assert len(keys) == len(values)


@PROP
@given(RULE_NAMES, st.dictionaries(NAMES, st.tuples(SCALARS, SCALARS), max_size=3),
       st.dictionaries(NAMES, st.tuples(SCALARS, SCALARS), max_size=3))
def test_tuple_valued_kwargs_are_distinguished(name, a, b):
    same = identity(a) == identity(b)
    assert (task_key(name, a) == task_key(name, b)) == same


# --- matrix normalisation ----------------------------------------------------

AXIS_VALUES = st.lists(st.one_of(st.integers(-3, 3), st.text(max_size=3), st.booleans()),
                       min_size=0, max_size=3)
MATRICES = st.dictionaries(st.sampled_from(list('abcd')), AXIS_VALUES, min_size=1, max_size=3)


def task_set(rule):
    """{key: kwargs} for a rule's expansion."""
    return {t.key: t.kwargs for t in expand_rule(rule)}


@PROP
@given(MATRICES)
def test_expansion_count_is_the_product_of_value_counts(matrix):
    expected = math.prod(len(v) for v in matrix.values())
    combos = resolve_matrix(matrix)
    assert len(combos) == expected
    # Repeated axis values give repeated combinations, hence the same key:
    # the task set is the set of distinct (typed) combinations.
    distinct = {tuple(sorted(identity(c).items())) for c in combos}
    assert len(task_set(make_rule(matrix=matrix))) == len(distinct)


@PROP
@given(MATRICES)
def test_dict_form_equals_the_list_of_dicts_form(matrix):
    keys = list(matrix)
    as_list = [dict(zip(keys, combo)) for combo in itertools.product(*matrix.values())]
    assert task_set(make_rule(matrix=matrix)) == task_set(make_rule(matrix=as_list))


@PROP
@given(MATRICES, st.integers(0, 5))
def test_matrix_key_order_does_not_change_keys(matrix, seed):
    shuffled = reorder(matrix, seed)
    assert task_set(make_rule(matrix=matrix)) == task_set(make_rule(matrix=shuffled))


@PROP
@given(st.lists(st.tuples(st.integers(-3, 3), st.text(max_size=3)), max_size=4),
       st.lists(st.booleans(), min_size=1, max_size=2))
def test_tuple_key_equals_the_equivalent_list_of_dicts(pairs, flags):
    tuple_form = {('a', 'b'): pairs, 'c': flags}
    list_form = [{'a': a, 'b': b, 'c': c} for (a, b) in pairs for c in flags]
    assert len(resolve_matrix(tuple_form)) == len(pairs) * len(flags)
    assert task_set(make_rule(matrix=tuple_form)) == task_set(make_rule(matrix=list_form))


# --- paths_digest ------------------------------------------------------------

PATHS = st.text(alphabet='abc/._01', min_size=1, max_size=6)
SPECS = st.dictionaries(NAMES, PATHS, max_size=4)


def digest_of(inputs, outputs):
    """paths_digest of a task whose resolved io are the given dicts (callables
    returning them, so they are used as they are)."""
    r = make_rule(inputs=lambda: inputs, outputs=lambda: outputs)
    return paths_digest(Task(rule=r))


def formula(inputs, outputs):
    """The 0.9 formula: sorted (side, repr(key), str(value)) triples."""
    items = sorted([('i', repr(k), str(v)) for k, v in inputs.items()]
                   + [('o', repr(k), str(v)) for k, v in outputs.items()])
    return f'{len(inputs)}:{len(outputs)}:{sha1(repr(items).encode()).hexdigest()}'


@PROP
@given(SPECS, SPECS, st.integers(0, 5))
def test_digest_ignores_dict_order(i, o, seed):
    assert digest_of(i, o) == digest_of(reorder(i, seed), reorder(o, seed + 1))


@PROP
@given(SPECS, SPECS, NAMES, PATHS)
def test_digest_changes_when_a_path_changes(i, o, key, new):
    if not i:
        i = {key: 'x'}
    k = sorted(i)[0]
    changed = {**i, k: i[k] + new}
    assert digest_of(i, o) != digest_of(changed, o)
    assert digest_of(o, i) != digest_of(o, changed)


@PROP
@given(SPECS, SPECS)
def test_digest_matches_the_0_9_formula_for_strings(i, o):
    assert digest_of(i, o) == formula(i, o)


@PROP
@given(st.dictionaries(NAMES, st.lists(PATHS, max_size=3), max_size=3),
       st.dictionaries(NAMES, PATHS, max_size=3))
def test_digest_matches_the_0_9_formula_for_lists_of_strings_and_paths(i, o):
    as_paths = {k: [Path(p) for p in v] for k, v in i.items()}
    assert digest_of(i, o) == formula(i, o)
    assert digest_of(as_paths, o) == formula(as_paths, o)


# --- io / uses renderings ----------------------------------------------------

IO_SPECS = st.dictionaries(NAMES, PATHS, min_size=1, max_size=4)


@PROP
@given(IO_SPECS, st.integers(0, 5))
def test_io_part_is_stable_and_ignores_key_order(spec, seed):
    assert io_part(spec) == io_part(dict(spec))
    assert io_part(spec) == io_part(reorder(spec, seed))


@PROP
@given(IO_SPECS, PATHS)
def test_changed_template_changes_io_part(spec, extra):
    k = sorted(spec)[0]
    assert io_part(spec) != io_part({**spec, k: spec[k] + extra})
    assert io_part(spec) != io_part({**spec, 'zz_new': extra})


USES_VALUES = st.one_of(st.integers(), st.text(max_size=4), st.lists(st.integers(), max_size=3),
                        st.dictionaries(st.text(max_size=3), st.integers(), max_size=3))


@PROP
@given(st.dictionaries(NAMES, USES_VALUES, max_size=4), st.integers(0, 5))
def test_uses_renderings_are_stable_and_ignore_order(uses, seed):
    shuffled = reorder(uses, seed)
    assert uses_parts(uses) == uses_parts(shuffled)
    assert uses_hash(uses) == uses_hash(shuffled) == uses_hash(dict(uses))
    assert list(uses_parts(uses)) == sorted(uses)


@PROP
@given(st.dictionaries(NAMES, st.integers(), min_size=1, max_size=4), st.integers(1, 5))
def test_changed_uses_value_changes_the_hash(uses, delta):
    k = sorted(uses)[0]
    assert uses_hash(uses) != uses_hash({**uses, k: uses[k] + delta})
