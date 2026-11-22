"""Lazy Task — a pure value object identified by (rule, kwargs).

Inputs/outputs are resolved on first access, never at construction: for 1e6
tasks you only pay for the ones you touch. DB state (status, timestamps)
lives in TaskRecord, returned by the metadata backend — not here.
"""
import inspect
from dataclasses import dataclass, field
from functools import cached_property
from hashlib import sha1

from .exceptions import SignatureError
from .rule import Rule
from .tokens import OutputToken, as_token


def _format_item(v, kwargs):
    return v.format(**kwargs) if isinstance(v, OutputToken) else str(v).format(**kwargs)


def _format_value(name, v, kwargs):
    """One spec value for one task: an item, or a flat list/tuple of items
    each formatted (0.10 decision D8)."""
    if isinstance(v, (list, tuple)):
        return type(v)(_format_item(e, kwargs) for e in check_flat(name, v))
    return _format_item(v, kwargs)


def check_flat(name, value):
    """A list/tuple value must be flat: one level of paths or tokens."""
    for e in value:
        if isinstance(e, (list, tuple, dict, set)):
            raise SignatureError(
                f'{name}: a list of paths must be flat (got a nested '
                f'{type(e).__name__}); use separate keys instead')
    return value


def _wrap(name, v):
    if isinstance(v, (list, tuple)):
        return type(v)(as_token(e) for e in check_flat(name, v))
    return as_token(v)


def _resolve(spec, kwargs, wrap_tokens):
    """Resolve an inputs/outputs spec to a concrete dict for one task.
    A value is one path/token, or a flat list or tuple of them; outputs
    become tokens element by element."""
    if spec is None:
        return {}
    if callable(spec) and not isinstance(spec, dict):
        params = inspect.signature(spec).parameters
        resolved = spec(**{k: v for k, v in kwargs.items() if k in params})
    else:
        resolved = {k: _format_value(k, v, kwargs) for k, v in spec.items()}
    if wrap_tokens:
        resolved = {k: _wrap(k, v) for k, v in resolved.items()}
    return resolved


def flat_items(mapping):
    """(label, item) for every path/token in a resolved inputs/outputs
    dict: a plain value under its key, list elements as 'key[i]'. The one way
    remake walks a task's paths (lint, checks, task-info, dependencies)."""
    for k, v in mapping.items():
        if isinstance(v, (list, tuple)):
            for i, e in enumerate(v):
                yield f'{k}[{i}]', e
        else:
            yield k, v


def flat_values(mapping):
    """Every path/token in a resolved inputs/outputs dict (see flat_items)."""
    return [item for _, item in flat_items(mapping)]


def task_key(rule_name, kwargs):
    kwargs_repr = repr(dict(sorted(kwargs.items())))
    return sha1(f'{rule_name}:{kwargs_repr}'.encode()).hexdigest()


@dataclass(eq=False)
class Task:
    rule: Rule
    kwargs: dict = field(default_factory=dict)
    # The key this task had before its matrix values were canonicalised
    # (numpy scalars, enums: review 2026-09-24 M19), when that differs. The
    # backend finds and re-keys a record stored under it, so canonical keys
    # don't orphan existing state.
    legacy_key: str = None
    # Why the planner chose to run this task (a category: 'never-run',
    # 'code-changed', 'upstream-rerun', ...; planner.plan), recorded in the
    # task's run history. None = not planned (run-task, an export).
    rerun_reason: str = field(default=None, compare=False, repr=False)

    @cached_property
    def key(self):
        return task_key(self.rule.name, self.kwargs)

    @cached_property
    def inputs(self):
        return _resolve(self.rule.inputs, self.kwargs, wrap_tokens=False)

    @cached_property
    def outputs(self):
        return _resolve(self.rule.outputs, self.kwargs, wrap_tokens=True)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, Task) and self.key == other.key

    def __repr__(self):
        kstr = ', '.join(f'{k}={v}' for k, v in self.kwargs.items())
        return f'{self.key[:8]} {self.rule.name}[{kstr}]'


def _digest_str(v):
    """str(v), with list/tuple elements as their path strings: identical to
    0.9's str(v) for lists of strings (no rerun on upgrade), and stable for
    lists of tokens, whose repr would otherwise leak into the digest."""
    if isinstance(v, (list, tuple)):
        return repr(type(v)(str(e) for e in v))
    return str(v)


def paths_digest(task):
    """A digest of a task's resolved input and output paths (review
    2026-09-24 M8), as '<ninputs>:<noutputs>:<sha1>', or None if they can't
    be resolved. Order-insensitive (keys sorted, as the io hash ignores dict
    order) and on the paths as declared, not resolved on disk, so moving the
    project doesn't change it."""
    try:
        inputs = task.inputs
        outputs = task.outputs
    except Exception:
        return None
    items = sorted(
        [('i', repr(k), _digest_str(v)) for k, v in inputs.items()]
        + [('o', repr(k), _digest_str(v)) for k, v in outputs.items()]
    )
    digest = sha1(repr(items).encode()).hexdigest()
    return f'{len(inputs)}:{len(outputs)}:{digest}'
