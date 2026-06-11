"""Free-variable analysis of rule functions.

A rule function's result must be fully determined by its inputs, kwargs and
declared `uses`. Names reached from outer scope (module globals, closures)
that are not declared in `uses` are reported: as a `ScopeWarning` by
default, or a `ScopeError` in strict mode.

This is detection only — there is no runtime scope isolation. Names in
`uses` are injected into the function's globals at execution time (see
`exec_function`), so rule code refers to them directly.
"""
import builtins
import dis
import inspect
import sys
import types
import warnings

from .exceptions import ScopeError

_BUILTIN_NAMES = frozenset(dir(builtins))
_STDLIB_MODULE_NAMES = frozenset(getattr(sys, 'stdlib_module_names', ()))


class ScopeWarning(UserWarning):
    pass


def _loaded_globals(code):
    """Names loaded as globals by a code object, recursing into nested code
    objects (comprehensions, lambdas, nested defs)."""
    names = set()
    for instr in dis.get_instructions(code):
        if instr.opname == 'LOAD_GLOBAL':
            names.add(instr.argval)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _loaded_globals(const)
    return names


def undeclared_names(fn, uses):
    """Names `fn` reaches from outer scope that are not covered by `uses`.

    Safe (not reported): builtins, names declared in `uses`, globals that
    resolve to modules (changing libraries is environment, not code), and
    names not resolvable at all (left to fail naturally at run time).
    """
    names = _loaded_globals(fn.__code__) | set(fn.__code__.co_freevars)
    undeclared = []
    for name in sorted(names):
        if name in _BUILTIN_NAMES or name in _STDLIB_MODULE_NAMES:
            continue
        if name in uses:
            continue
        if name in fn.__globals__:
            value = fn.__globals__[name]
            if isinstance(value, types.ModuleType):
                continue
            # Objects imported from the stdlib (e.g. Path, datetime) are
            # environment, not trackable code.
            defining_module = getattr(value, '__module__', '') or ''
            if defining_module.split('.')[0] in _STDLIB_MODULE_NAMES:
                continue
            undeclared.append(name)
        elif name in fn.__code__.co_freevars:
            undeclared.append(name)
    return undeclared


def check_scope(fn, uses, strict):
    """Warn about (or, in strict mode, raise on) undeclared outer-scope names."""
    undeclared = undeclared_names(fn, uses)
    if not undeclared:
        return
    msg = (
        f'{fn.__name__} uses undeclared name(s) from outer scope: '
        f'{", ".join(undeclared)}. Declare them in uses= to track changes.'
    )
    if strict:
        raise ScopeError(msg)
    warnings.warn(msg, ScopeWarning, stacklevel=3)


def uses_hash(uses):
    """Stable string representing the current state of a `uses` dict.

    Functions are represented by their source (so a body change is a
    change); plain values by repr.
    """
    parts = []
    for name in sorted(uses):
        value = uses[name]
        if callable(value):
            parts.append(f'{name}={inspect.getsource(value)}')
        else:
            parts.append(f'{name}={value!r}')
    return '\n'.join(parts)


def exec_function(fn, uses):
    """Return `fn` with `uses` entries available as globals.

    Rule code refers to uses names directly (e.g. `threshold` for
    uses={'threshold': THRESHOLD}); this is where they are bound.
    """
    if not uses:
        return fn
    exec_globals = dict(fn.__globals__)
    exec_globals.update(uses)
    new_fn = types.FunctionType(
        fn.__code__, exec_globals, fn.__name__, fn.__defaults__, fn.__closure__
    )
    new_fn.__kwdefaults__ = fn.__kwdefaults__
    return new_fn
