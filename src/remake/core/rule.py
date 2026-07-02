"""The module-level @rule decorator and the Rule descriptor it produces.

Decoration does exactly two things: validate the signature contract and run
scope analysis. Registration with a Remake instance happens separately
(rules are free-standing, importable objects).
"""
import inspect
from dataclasses import dataclass, field
from typing import Callable, Optional, Union

from .exceptions import SignatureError
from .scope import check_scope, check_shadowing, function_source


@dataclass(eq=False)
class Rule:
    fn: Callable
    inputs: Union[dict, Callable, None] = None
    outputs: Union[dict, Callable, None] = None
    matrix: Union[dict, list, Callable, None] = None
    depends_on: list = field(default_factory=list)
    uses: dict = field(default_factory=dict)
    strict_scope: Optional[bool] = None  # None -> inherit Remake default
    config: dict = field(default_factory=dict)
    _name: Optional[str] = None
    # Set by Remake at registration:
    remake: object = None

    @property
    def name(self):
        return self._name if self._name is not None else self.fn.__name__

    @property
    def source(self):
        """Source representation of each part, for metadata storage and
        change detection. Callables by source, dicts by repr."""

        def part_source(part):
            if part is None:
                return ''
            if callable(part):
                return function_source(part)
            return repr(part)

        return {
            'inputs': part_source(self.inputs),
            'outputs': part_source(self.outputs),
            'run': function_source(self.fn),
        }

    def __repr__(self):
        return f'Rule({self.name})'


def deferrable(matrix_fn):
    """Mark a matrix callable as deferrable: it derives its task list from
    upstream outputs and may raise `Defer`.

    Only `@deferrable` matrices may defer — raising `Defer` from an unmarked
    matrix is an error (it makes the dynamic contract explicit at the call
    site). The planner also defers a `@deferrable` rule when an upstream is
    rerunning this wave, so its matrix never expands from a stale output.
    """
    matrix_fn._remake_deferrable = True
    return matrix_fn


def is_deferrable(matrix):
    """True if `matrix` is a callable marked with `@deferrable`."""
    return callable(matrix) and getattr(matrix, '_remake_deferrable', False)


def _matrix_keys(matrix):
    """Matrix parameter names, or None if not knowable at decoration time."""
    if matrix is None:
        return set()
    if callable(matrix):
        return None
    if isinstance(matrix, dict):
        # Tuple keys bind several kwargs together (the grouped form); flatten
        # them so the signature check sees the individual parameter names.
        keys = set()
        for k in matrix:
            keys.update(k if isinstance(k, tuple) else (k,))
        return keys
    if isinstance(matrix, list):
        return set(matrix[0].keys()) if matrix else set()
    raise SignatureError(f'matrix must be dict, list[dict] or callable, not {type(matrix)}')


def _template_fields(value):
    """Base names of the format fields in a template (e.g. 'a/{n:03d}/{m}.nc'
    -> {'n', 'm'}; attribute/index access reduces to the base name)."""
    import string

    fields = set()
    for _, field_name, _, _ in string.Formatter().parse(str(value)):
        if field_name is not None:
            fields.add(field_name.split('.')[0].split('[')[0])
    return fields


def check_io_spec(rule_name, part_name, spec, matrix_keys):
    """Validate an inputs/outputs spec against the matrix keys.

    A mismatch here is rule *plumbing*, not a task problem: resolution is
    per-task and lazy, so without this check an inputs function whose
    signature doesn't match the matrix (or a template naming a non-existent
    kwarg) fails identically for every task, N times, at run time. Raise
    once, early, naming the rule instead. Called at decoration for static
    matrices and at first expansion for callable ones (the same split as the
    run-function signature check).
    """
    if spec is None:
        return
    if callable(spec) and not isinstance(spec, dict):
        params = inspect.signature(spec).parameters
        required = [p.name for p in params.values()
                    if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD,
                                  p.KEYWORD_ONLY)
                    and p.default is p.empty]
        missing = sorted(n for n in required if n not in matrix_keys)
        if missing:
            fname = getattr(spec, '__name__', repr(spec))
            raise SignatureError(
                f'{rule_name}: {part_name} function {fname!r} requires '
                f'parameter(s) {missing} not provided by the matrix '
                f'(keys: {sorted(matrix_keys)}). It is called with the matrix '
                f'kwargs it names, so every task of this rule would fail '
                f'identically — fix the function signature or the matrix.'
            )
    else:
        for key, value in spec.items():
            missing = sorted(f for f in _template_fields(value)
                             if not f or f not in matrix_keys)
            if missing:
                raise SignatureError(
                    f'{rule_name}: {part_name} template {key}={str(value)!r} '
                    f'references field(s) {missing} not provided by the matrix '
                    f'(keys: {sorted(matrix_keys)}). Every task of this rule '
                    f'would fail identically at resolution — fix the template '
                    f'or the matrix.'
                )


def _validate_signature(fn, inputs, outputs, matrix):
    """The signature contract: def fn([inputs,] [outputs,] <matrix keys>)."""
    if isinstance(inputs, dict) and not inputs:
        raise SignatureError(f'{fn.__name__}: inputs={{}} is ambiguous — omit the argument')
    if isinstance(outputs, dict) and not outputs:
        raise SignatureError(f'{fn.__name__}: outputs={{}} is ambiguous — omit the argument')

    params = list(inspect.signature(fn).parameters.values())
    for p in params:
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise SignatureError(f'{fn.__name__}: *args/**kwargs not allowed in rule functions')
    names = [p.name for p in params]

    expected_head = []
    if inputs is not None:
        expected_head.append('inputs')
    if outputs is not None:
        expected_head.append('outputs')
    if names[: len(expected_head)] != expected_head:
        raise SignatureError(
            f'{fn.__name__}: signature must start with ({", ".join(expected_head)}, ...); '
            f'got ({", ".join(names)})'
        )
    for special in ('inputs', 'outputs'):
        if special in names and special not in expected_head:
            raise SignatureError(
                f'{fn.__name__} takes "{special}" but does not declare {special}='
            )

    rest = set(names[len(expected_head):])
    matrix_keys = _matrix_keys(matrix)
    if matrix_keys is None:
        # Callable matrix: parameter names unknown until planning; the run
        # signature and the inputs/outputs specs are checked at expansion
        # time instead (dag._check_expanded_kwargs).
        return
    if rest != matrix_keys:
        raise SignatureError(
            f'{fn.__name__}: parameters {sorted(rest)} do not match matrix keys '
            f'{sorted(matrix_keys)}'
        )
    check_io_spec(fn.__name__, 'inputs', inputs, matrix_keys)
    check_io_spec(fn.__name__, 'outputs', outputs, matrix_keys)


def rule(
    *,
    inputs=None,
    outputs=None,
    matrix=None,
    depends_on=None,
    uses=None,
    strict_scope=None,
    config=None,
    name=None,
):
    """Define a rule. Returns a free-standing Rule object (not the function).

    See remake3_design.md for the full parameter semantics.
    """

    def decorator(fn):
        uses_ = dict(uses) if uses else {}
        _validate_signature(fn, inputs, outputs, matrix)
        # Rule-level strict_scope=True errors now; None defers strictness to
        # registration (warnings are still emitted now).
        check_scope(fn, uses_, strict=bool(strict_scope))
        check_shadowing(fn, uses_)
        rule_obj = Rule(
            fn=fn,
            inputs=inputs,
            outputs=outputs,
            matrix=matrix,
            depends_on=list(depends_on) if depends_on else [],
            uses=uses_,
            strict_scope=strict_scope,
            config=dict(config) if config else {},
            _name=name,
        )
        # Surface the decorated function's docstring on the Rule itself so
        # `help(rule)` / `rule.__doc__` see through to the user's function
        # (the Rule object replaces the function, so wraps doesn't apply).
        rule_obj.__doc__ = fn.__doc__
        return rule_obj

    return decorator
