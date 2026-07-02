"""Free-variable analysis of rule functions.

A rule function's result must be fully determined by its inputs, kwargs and
declared `uses`. Names reached from outer scope (module globals, closures)
that are not declared in `uses` are reported: as a `ScopeWarning` by
default, or a `ScopeError` in strict mode.

This is detection only — there is no runtime scope isolation. Names in
`uses` are injected into the function's globals at execution time (see
`exec_function`), so rule code refers to them directly.
"""
import ast
import builtins
import dis
import inspect
import sys
import types
import warnings

from ..util.code_compare import dedent
from .exceptions import ScopeError

_BUILTIN_NAMES = frozenset(dir(builtins))
_STDLIB_MODULE_NAMES = frozenset(getattr(sys, 'stdlib_module_names', ()))


class ScopeWarning(UserWarning):
    pass


def _loaded_globals(code):
    """Names loaded as globals by a code object, recursing into nested code
    objects (comprehensions, lambdas, nested defs)."""
    names = set()
    # A global read compiles to LOAD_GLOBAL carrying the name as its argument
    # (locals/args use LOAD_FAST, so they are excluded for free).
    for instr in dis.get_instructions(code):
        if instr.opname == 'LOAD_GLOBAL':
            names.add(instr.argval)
    # Comprehensions, lambdas and nested defs each compile to their own code
    # object stored in co_consts; their LOAD_GLOBALs live there, not in the
    # parent, so recurse to catch a global only referenced inside them.
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
    # Resolve closed-over names to their values up front: each freevar is
    # backed by a cell in __closure__, and we need the value (not just the
    # name) to apply the environment filter below. An *empty* cell — the name
    # is closed over but its enclosing assignment never ran — raises ValueError
    # on cell_contents; skip it here, and it falls through to the freevar
    # branch below (reported, since we have no value to inspect).
    closure_values = {}
    if fn.__closure__:
        for name, cell in zip(fn.__code__.co_freevars, fn.__closure__):
            try:
                closure_values[name] = cell.cell_contents
            except ValueError:  # cell not yet filled (recursive def)
                pass

    # The two ways a function reaches outer scope: module globals (LOAD_GLOBAL)
    # and closure free variables (co_freevars). Their union is every name it
    # could pull from a higher scope.
    names = _loaded_globals(fn.__code__) | set(fn.__code__.co_freevars)
    undeclared = []
    for name in sorted(names):
        # Builtins and stdlib module names are environment, not the rule's code.
        if name in _BUILTIN_NAMES or name in _STDLIB_MODULE_NAMES:
            continue
        # Already opted in to change tracking.
        if name in uses:
            continue
        # Closed-over value we could read: report unless it's environment
        # (a module, or a stdlib-defined object like Path/datetime).
        if name in closure_values:
            if _is_environment(closure_values[name]):
                continue
            undeclared.append(name)
        # Module global: same environment check.
        elif name in fn.__globals__:
            if _is_environment(fn.__globals__[name]):
                continue
            undeclared.append(name)
        # In co_freevars but with no inspectable value (empty cell above):
        # a real outer-scope reference, so report it rather than drop it.
        elif name in fn.__code__.co_freevars:
            undeclared.append(name)
        # Otherwise: a LOAD_GLOBAL name with no current binding (not in globals
        # or closure) — left to fail naturally at run time, not reported.
    return undeclared


def _is_environment(value):
    """Modules, and objects imported from the stdlib (e.g. Path, datetime),
    are environment, not trackable code."""
    # A module reference itself (e.g. `import numpy as np`).
    if isinstance(value, types.ModuleType):
        return True
    # An object whose defining module is rooted in the stdlib — its top-level
    # package name (before the first dot) is a known stdlib module.
    defining_module = getattr(value, '__module__', '') or ''
    return defining_module.split('.')[0] in _STDLIB_MODULE_NAMES


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


def function_source(fn):
    """Source of fn, for change detection. Callables defined where source is
    unavailable (REPL, exec) fall back to a bytecode digest — kept as a
    parseable string literal so AST comparison still works. Sourceless
    callables without __code__ (e.g. scipy distribution objects, classes
    defined in a REPL) fall back to a label naming their type: stable
    across processes (unlike repr, which embeds a memory address), but
    body changes go undetected."""
    try:
        return inspect.getsource(fn)
    except (OSError, TypeError):
        code = getattr(fn, '__code__', None)
        if code is None:
            cls = type(fn)
            return f"'<unsourced:{cls.__module__}.{cls.__qualname__}>'"

        from hashlib import sha1

        return f"'<bytecode:{sha1(code.co_code).hexdigest()}>'"


def _normalised_source(fn):
    """AST-normalised source: cosmetic changes (whitespace, comments) do not
    change it, so string equality of the result is AST comparison."""
    src = function_source(fn)
    try:
        return ast.dump(ast.parse(dedent(src)))
    except SyntaxError:
        # e.g. a lambda whose getsource line isn't parseable standalone.
        return src


def _normalised_src(src):
    """AST-normalise an already-extracted source string (cosmetic changes do
    not change it). Empty stays empty; unparseable falls back to the raw."""
    if not src:
        return ''
    try:
        return ast.dump(ast.parse(dedent(src)))
    except SyntaxError:
        return src


def io_hash(rule):
    """Stable string for a rule's inputs/outputs declarations, AST-normalised
    the same way run code and uses are. Editing the inputs/outputs spec (the
    dict or the callable that defines dataflow) changes it, so a completed
    task is rerun — closing the gap where only run code and uses were tracked.

    Note: a pure output-*path* change driven by a global constant the callable
    reads at run time is not captured here (the source is unchanged) — that is
    the `uses` tracking domain, same caveat as run code."""
    src = rule.source
    return (
        f"inputs={_normalised_src(src['inputs'])}\n"
        f"outputs={_normalised_src(src['outputs'])}"
    )


def uses_parts(uses):
    """{name: rendered-value} for a `uses` dict, name-sorted.

    Functions are rendered as their AST-normalised source (a body change is
    a change, a comment is not — tracking is one level deep: helpers a
    uses-function calls must themselves be declared in uses); plain values
    by repr.
    """
    parts = {}
    for name in sorted(uses):
        value = uses[name]
        if callable(value):
            parts[name] = _normalised_source(value)
        else:
            parts[name] = repr(value)
    return parts


def raw_uses_parts(uses):
    """{name: (raw-rendering, kind)} for a `uses` dict, name-sorted — the
    display-side counterpart of `uses_parts` (which renders normalised AST
    for change detection). Raw source diffs readably; the `kind` marker
    tells the display layer what it is looking at:

    - 'source': a callable whose raw source is available — diffable.
    - 'value': a plain value, rendered by repr — trivially diffable.
    - 'bytecode': a callable with no retrievable source (REPL/exec, C
      function) — the digest/type label from `function_source`; not
      diffable, display as "source unavailable".
    """
    parts = {}
    for name in sorted(uses):
        value = uses[name]
        if callable(value):
            try:
                parts[name] = (inspect.getsource(value), 'source')
            except (OSError, TypeError):
                parts[name] = (function_source(value), 'bytecode')
        else:
            parts[name] = (repr(value), 'value')
    return parts


def uses_hash(uses):
    """Stable string representing the current state of a `uses` dict: one
    `name=<rendered>` line per key (see `uses_parts`)."""
    return '\n'.join(f'{name}={rendered}' for name, rendered in uses_parts(uses).items())


def parse_uses_hash(stored):
    """Inverse of `uses_hash`: recover {name: rendered-value} from a stored
    hash string. A rendered value may span several lines (a callable whose
    source didn't parse standalone, e.g. a lambda), so a new key begins only
    at a line matching `<identifier>=`; other lines continue the prior value.
    """
    parts = {}
    name = None
    for line in stored.split('\n'):
        head, sep, rest = line.partition('=')
        if sep and head.isidentifier():
            name = head
            parts[name] = rest
        elif name is not None:
            parts[name] += '\n' + line
    return parts


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
