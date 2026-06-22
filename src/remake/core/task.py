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


@dataclass(eq=False)
class Task:
    rule: Rule
    kwargs: dict = field(default_factory=dict)

    @cached_property
    def key(self):
        kwargs_repr = repr(dict(sorted(self.kwargs.items())))
        return sha1(f'{self.rule.name}:{kwargs_repr}'.encode()).hexdigest()

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
