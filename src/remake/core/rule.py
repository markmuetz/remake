"""The module-level @rule decorator and the Rule descriptor it produces.

Decoration does exactly two things: validate the signature contract and run
scope analysis. Registration with a Remake instance happens separately
(rules are free-standing, importable objects).
"""
import inspect
from dataclasses import dataclass, field
from typing import Callable, Optional, Union

from .exceptions import SignatureError
from .scope import check_scope, function_source


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
    # Set by Remake at registration:
    remake: object = None

    @property
    def name(self):
        return self.fn.__name__

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
        # Callable matrix: parameter names unknown until planning; checked
        # at expansion time instead.
        return
    if rest != matrix_keys:
        raise SignatureError(
            f'{fn.__name__}: parameters {sorted(rest)} do not match matrix keys '
            f'{sorted(matrix_keys)}'
        )


def rule(
    *,
    inputs=None,
    outputs=None,
    matrix=None,
    depends_on=None,
    uses=None,
    strict_scope=None,
    config=None,
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
        return Rule(
            fn=fn,
            inputs=inputs,
            outputs=outputs,
            matrix=matrix,
            depends_on=list(depends_on) if depends_on else [],
            uses=uses_,
            strict_scope=strict_scope,
            config=dict(config) if config else {},
        )

    return decorator
