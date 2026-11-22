"""Lazy Task — a pure value object identified by (rule, kwargs).

Inputs/outputs are resolved on first access, never at construction: for 1e6
tasks you only pay for the ones you touch. DB state (status, timestamps)
lives in TaskRecord, returned by the metadata backend — not here.
"""
import inspect
from dataclasses import dataclass, field
from functools import cached_property
from hashlib import sha1

from .rule import Rule
from .tokens import OutputToken, as_token


def _resolve(spec, kwargs, wrap_tokens):
    """Resolve an inputs/outputs spec to a concrete dict for one task."""
    if spec is None:
        return {}
    if callable(spec) and not isinstance(spec, dict):
        params = inspect.signature(spec).parameters
        resolved = spec(**{k: v for k, v in kwargs.items() if k in params})
    else:
        resolved = {}
        for k, v in spec.items():
            if isinstance(v, OutputToken):
                resolved[k] = v.format(**kwargs)
            else:
                resolved[k] = str(v).format(**kwargs)
    if wrap_tokens:
        resolved = {k: as_token(v) for k, v in resolved.items()}
    return resolved


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
        [('i', repr(k), str(v)) for k, v in inputs.items()]
        + [('o', repr(k), str(v)) for k, v in outputs.items()]
    )
    digest = sha1(repr(items).encode()).hexdigest()
    return f'{len(inputs)}:{len(outputs)}:{digest}'
