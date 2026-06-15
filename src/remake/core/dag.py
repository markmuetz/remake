"""Rule-level DAG construction and task expansion. Pure functions, no I/O.

The rule DAG is the only graph remake builds — there is no task-level DAG.
See remake3_design.md, "No task-level DAG".
"""
import itertools

import networkx as nx

from .exceptions import SignatureError
from .task import Task


def build_rule_dag(rules):
    """Directed rule-level DAG from explicit depends_on declarations."""
    g = nx.DiGraph()
    for rule in rules:
        g.add_node(rule)
        for dep in rule.depends_on:
            g.add_edge(dep, rule)
    if not nx.is_directed_acyclic_graph(g):
        cycle = nx.find_cycle(g)
        raise ValueError(f'Rule dependencies contain a cycle: {cycle}')
    return g


def resolve_matrix(matrix):
    """Normalise all matrix forms to the canonical list[dict].

    May raise MatrixNotReady if matrix is a callable whose required upstream
    outputs do not yet exist.
    """
    if matrix is None:
        return [{}]
    if callable(matrix) and not isinstance(matrix, (dict, list)):
        return matrix()
    if isinstance(matrix, list):
        return matrix
    # dict: cartesian product of axes. A scalar key is one kwarg per value; a
    # tuple key binds several kwargs together per value (the remake2 grouped
    # form), letting you supply an explicit, pre-filtered sequence of combos.
    axes = []
    for key, values in matrix.items():
        if isinstance(key, tuple):
            for v in values:
                if not isinstance(v, tuple) or len(v) != len(key):
                    raise SignatureError(
                        f'matrix tuple key {key} expects value tuples of length '
                        f'{len(key)}, got {v!r}'
                    )
            axes.append([dict(zip(key, v)) for v in values])
        else:
            axes.append([{key: v} for v in values])
    return [
        {k: v for axis in combo for k, v in axis.items()}
        for combo in itertools.product(*axes)
    ]


def expand_rule(rule, predicate=None):
    """Expand the matrix for one rule into Task objects (no I/O).

    predicate: optional callable(namespace) -> bool, applied before Task
    construction so filtered-out tasks are never created. The namespace is
    the task kwargs plus 'rule' (the rule name — it wins over a matrix key
    of the same name), so queries can select by rule: "rule == 'extract'".
    """
    return list(iter_expand_rule(rule, predicate))


def iter_expand_rule(rule, predicate=None):
    """Generator form of expand_rule — yields Tasks one at a time."""
    kwargs_list = resolve_matrix(rule.matrix)
    if callable(rule.matrix) and kwargs_list:
        # Deferred half of the signature contract: parameter names were
        # unknowable at decoration time for callable matrices.
        _check_expanded_kwargs(rule, kwargs_list[0])
    for kw in kwargs_list:
        if predicate is None or predicate({**kw, 'rule': rule.name}):
            yield Task(rule=rule, kwargs=kw)


def _check_expanded_kwargs(rule, kwargs):
    import inspect

    names = list(inspect.signature(rule.fn).parameters)
    expected = [n for n in names if n not in ('inputs', 'outputs')]
    if set(expected) != set(kwargs):
        raise SignatureError(
            f'{rule.name}: parameters {sorted(expected)} do not match resolved '
            f'matrix keys {sorted(kwargs)}'
        )
