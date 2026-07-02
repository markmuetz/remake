"""Rule-level DAG construction and task expansion. Pure functions, no I/O.

The rule DAG is the only graph remake builds — there is no task-level DAG.
See remake3_design.md, "No task-level DAG".
"""
import itertools

import networkx as nx

from .exceptions import Defer, SignatureError
from .rule import is_deferrable
from .task import Task


def build_rule_dag(rules):
    """Directed rule-level DAG from explicit depends_on declarations."""
    rules_by_name = {rule.name: rule for rule in rules}
    g = nx.DiGraph()
    for rule in rules:
        g.add_node(rule)
        resolved = []
        for dep in rule.depends_on:
            if isinstance(dep, str):
                if dep not in rules_by_name:
                    raise ValueError(
                        f'{rule.name}: depends_on references unknown rule {dep!r}'
                    )
                dep = rules_by_name[dep]
            resolved.append(dep)
            g.add_edge(dep, rule)
        rule.depends_on = resolved
    if not nx.is_directed_acyclic_graph(g):
        cycle = nx.find_cycle(g)
        raise ValueError(f'Rule dependencies contain a cycle: {cycle}')
    return g


def resolve_matrix(matrix):
    """Normalise all matrix forms to the canonical list[dict].

    May raise Defer if matrix is a `@deferrable` callable whose required
    upstream outputs do not yet exist. A non-`@deferrable` matrix that raises
    Defer is a programming error (SignatureError) — the contract must be
    declared explicitly.
    """
    if matrix is None:
        return [{}]
    if callable(matrix) and not isinstance(matrix, (dict, list)):
        try:
            return matrix()
        except Defer:
            if not is_deferrable(matrix):
                name = getattr(matrix, '__name__', repr(matrix))
                raise SignatureError(
                    f'matrix callable {name!r} raised Defer but is not marked '
                    f'@deferrable. Decorate it with @deferrable to declare that '
                    f'it derives its task list from upstream outputs and so may '
                    f'defer.'
                ) from None
            raise
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


# Matrix kwarg values must survive the JSON round-trip used to ship SLURM
# job specs to compute nodes (a tuple/set comes back as a list, changing both
# Task.key and any kwarg-derived output path). Restrict them to JSON scalars,
# checked at plan time before anything is submitted. bool is an int subclass.
_SCALAR_TYPES = (str, int, float, bool, type(None))


def _check_scalar_kwargs(rule, kwargs):
    for key, value in kwargs.items():
        if not isinstance(value, _SCALAR_TYPES):
            raise SignatureError(
                f'{rule.name}: matrix value {key}={value!r} is a '
                f'{type(value).__name__}; matrix kwarg values must be scalars '
                f'(str, int, float, bool, None). Non-scalars do not survive the '
                f'JSON round-trip used for SLURM job specs (e.g. a tuple returns '
                f'as a list), which silently changes the task key and any output '
                f'path built from it. Encode the value as a string instead — e.g. '
                f'a short label, or a JSON string parsed inside the rule.'
            )


def iter_expand_rule(rule, predicate=None):
    """Generator form of expand_rule — yields Tasks one at a time."""
    kwargs_list = resolve_matrix(rule.matrix)
    if callable(rule.matrix) and kwargs_list:
        # Deferred half of the signature contract: parameter names were
        # unknowable at decoration time for callable matrices.
        _check_expanded_kwargs(rule, kwargs_list[0])
    for kw in kwargs_list:
        _check_scalar_kwargs(rule, kw)
        if predicate is None or predicate({**kw, 'rule': rule.name}):
            yield Task(rule=rule, kwargs=kw)


def _check_expanded_kwargs(rule, kwargs):
    import inspect

    from .rule import check_io_spec

    names = list(inspect.signature(rule.fn).parameters)
    expected = [n for n in names if n not in ('inputs', 'outputs')]
    if set(expected) != set(kwargs):
        raise SignatureError(
            f'{rule.name}: parameters {sorted(expected)} do not match resolved '
            f'matrix keys {sorted(kwargs)}'
        )
    check_io_spec(rule.name, 'inputs', rule.inputs, set(kwargs))
    check_io_spec(rule.name, 'outputs', rule.outputs, set(kwargs))
