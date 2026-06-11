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
    # {key: [values]} cartesian shorthand.
    combos = itertools.product(*matrix.values())
    return [dict(zip(matrix.keys(), combo)) for combo in combos]


def expand_rule(rule, predicate=None):
    """Expand the matrix for one rule into Task objects (no I/O).

    predicate: optional callable(kwargs) -> bool, applied before Task
    construction so filtered-out tasks are never created.
    """
    kwargs_list = resolve_matrix(rule.matrix)
    if callable(rule.matrix) and kwargs_list:
        # Deferred half of the signature contract: parameter names were
        # unknowable at decoration time for callable matrices.
        _check_expanded_kwargs(rule, kwargs_list[0])
    if predicate is not None:
        kwargs_list = [kw for kw in kwargs_list if predicate(kw)]
    return [Task(rule=rule, kwargs=kw) for kw in kwargs_list]


def _check_expanded_kwargs(rule, kwargs):
    import inspect

    names = list(inspect.signature(rule.fn).parameters)
    expected = [n for n in names if n not in ('inputs', 'outputs')]
    if set(expected) != set(kwargs):
        raise SignatureError(
            f'{rule.name}: parameters {sorted(expected)} do not match resolved '
            f'matrix keys {sorted(kwargs)}'
        )
