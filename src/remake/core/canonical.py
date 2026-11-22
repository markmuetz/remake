"""Canonical renderings of rule state, for change detection (0.9.0 item 2).

Change detection compares what a task last ran with against what it would
run with now. The renderings here make that comparison mean what it says:

- **Deterministic.** Sets are sorted and dict keys are sorted (dict order is
  not semantic under `==`), so `uses={'VARS': {'tas', 'pr'}}` no longer
  reruns on every invocation because string hashing is randomised per
  process (review 2026-09-24 H4).
- **Version-independent.** ASTs are dumped by our own dumper, which omits
  empty fields and positions, instead of `ast.dump`, whose output changed in
  Python 3.13 (H5).
- **Lossless where it matters.** numpy arrays hash their bytes (a repr
  truncates past 1000 elements and rounds floats, M4); pandas objects hash
  their values, index, columns and dtypes; `functools.partial`, numpy ufuncs,
  builtins and sourceless functions render by what they compute, not by a
  type label (M3).
- **Cosmetic changes are not changes.** Comments and whitespace never
  reach an AST. Docstrings do count (review L7 proposed dropping them; MM
  kept them, 2026-11-21: a docstring is part of the code a user reviews).

Stored strings carry a format marker (`FORMAT_MARKER`, the first line). A
stored string without it was written by remake <= 0.9.0a1; the planner
matches those through `legacy_*` (see `uses_match`/`io_match`) so upgrading
does not rerun everything, and restamps matched records to the new form.
"""
import ast
import dataclasses
import enum
import functools
import hashlib
import inspect
import json
import linecache
import marshal
import os
import sys
import textwrap
import types

FORMAT_MARKER = '#remake-canonical-v2'

# Fields that carry positions or are otherwise not semantic.
_SKIP_FIELDS = frozenset({'lineno', 'end_lineno', 'col_offset', 'end_col_offset',
                          'type_comment'})


# --- AST ---------------------------------------------------------------------

def dump_ast(node):
    """A version-independent dump of an AST: no positions, and empty fields
    ([] or None) omitted, so a field added in a later Python with an empty
    default (e.g. 3.12's `type_params`) doesn't change the result."""
    if isinstance(node, ast.AST):
        parts = []
        for name in node._fields:
            if name in _SKIP_FIELDS or not hasattr(node, name):
                continue
            value = getattr(node, name)
            if value is None and name != 'value':
                continue
            if isinstance(value, list) and not value:
                continue
            parts.append(f'{name}={dump_ast(value)}')
        return f'{type(node).__name__}({", ".join(parts)})'
    if isinstance(node, list):
        return '[' + ', '.join(dump_ast(n) for n in node) + ']'
    return repr(node)


def _is_rule_decorator(dec):
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Name):
        return target.id == 'rule'
    if isinstance(target, ast.Attribute):
        return target.attr == 'rule'
    return False


def normalise_def(tree, *, strip_rule_decorator=False):
    """Normalise a parsed module holding one def/class in place: for a rule
    function, drop the `@rule(...)` decorator and any decorators above it (H2: those configure or wrap the Rule object,
    not the function that runs). Decorators *below* `@rule` wrap the
    function itself and stay. Returns the tree."""
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if strip_rule_decorator:
                idx = next((i for i, d in enumerate(node.decorator_list)
                            if _is_rule_decorator(d)), None)
                if idx is not None:
                    node.decorator_list = node.decorator_list[idx + 1:]
            break
    return tree


# Reconstructing an AST from `ast.dump` text without eval: the dump is a
# Python expression of Call/List/Constant nodes, so parse it and rebuild the
# nodes it names. Used only to read legacy stored strings.

class _NotRebuildable(Exception):
    pass


def _rebuild(node):
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.args:
            raise _NotRebuildable
        cls = getattr(ast, node.func.id, None)
        if not (isinstance(cls, type) and issubclass(cls, ast.AST)):
            raise _NotRebuildable(node.func.id)
        fields = {kw.arg: _rebuild(kw.value) for kw in node.keywords}
        known = set(cls._fields)
        # Fields this Python's node class lacks (from another version's dump)
        # are dropped: dump_ast only renders this Python's fields anyway.
        obj = cls(**{k: v for k, v in fields.items() if k in known})
        return obj
    if isinstance(node, ast.List):
        return [_rebuild(e) for e in node.elts]
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name) and node.id == 'Ellipsis':
        return Ellipsis
    if (isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub)
            and isinstance(node.operand, ast.Constant)):
        return -node.operand.value
    raise _NotRebuildable(type(node).__name__)


def ast_from_dump(text):
    """The AST an `ast.dump()` string describes, or None if it isn't one we
    can rebuild (unknown node class, not a dump at all)."""
    try:
        expr = ast.parse(text, mode='eval').body
        return _rebuild(expr)
    except (SyntaxError, ValueError, RecursionError, MemoryError, _NotRebuildable,
            TypeError, AttributeError):
        return None


# --- sources -----------------------------------------------------------------

@functools.lru_cache(maxsize=64)
def _module_tree(path, mtime_ns):
    linecache.checkcache(path)  # the file may have changed since it was cached
    lines = linecache.getlines(path)
    if not lines:
        return None
    try:
        return ast.parse(''.join(lines), filename=path)
    except (SyntaxError, ValueError):
        return None


def _lambda_nodes(fn):
    """The Lambda node(s) in fn's source file that could be fn: those on its
    first line with its argument names. None if the file can't be read."""
    code = fn.__code__
    try:
        path = inspect.getsourcefile(fn)
        mtime_ns = os.stat(path).st_mtime_ns
    except (TypeError, OSError):
        return None
    tree = _module_tree(path, mtime_ns)
    if tree is None:
        return None
    argnames = list(code.co_varnames[:code.co_argcount + code.co_kwonlyargcount])
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Lambda) and node.lineno == code.co_firstlineno:
            a = node.args
            names = [x.arg for x in a.posonlyargs + a.args + a.kwonlyargs]
            if names == argnames:
                found.append(node)
    return found or None


def callable_source_ast(fn, *, strip_rule_decorator=False):
    """Canonical AST text of a function, lambda or class, or None when no
    source is available. A lambda's `getsource` is the whole line(s) it sits
    on (often an unparseable `@rule(inputs=lambda ...` fragment, and with the
    neighbouring arguments in it); here it is cut down to the lambda itself."""
    if getattr(fn, '__name__', None) == '<lambda>' and hasattr(fn, '__code__'):
        nodes = _lambda_nodes(fn)
        if nodes:
            # Several identical-signature lambdas on one line: all of them.
            return ' | '.join(dump_ast(n) for n in nodes)
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        return None
    try:
        tree = ast.parse(textwrap.dedent(src))
    except SyntaxError:
        return None
    return dump_ast(normalise_def(tree, strip_rule_decorator=strip_rule_decorator))


def _qualname(obj):
    return f'{getattr(obj, "__module__", "?")}.{getattr(obj, "__qualname__", getattr(obj, "__name__", "?"))}'


def _code_digest(code):
    """A sourceless function's identity: its full code object (constants,
    names and nested code included — not just co_code, review L8)."""
    try:
        return hashlib.sha256(marshal.dumps(code)).hexdigest()[:16]
    except ValueError:
        return hashlib.sha256(code.co_code).hexdigest()[:16]


# --- values ------------------------------------------------------------------

_MAX_DEPTH = 40


def _sorted_join(items):
    return ', '.join(sorted(items))


def _has_address(text):
    return ' at 0x' in text or 'object at ' in text


def canonical_value(value, *, project_dir=None, _depth=0):
    """A deterministic text for a `uses` value (see the module docstring).

    Plain values render as their repr, with dict keys and set elements
    sorted; most other objects as their repr too, as long as it is stable
    (no memory address). Types can opt in to their own rendering with a
    `__remake_hash__(self) -> str` method."""
    if _depth > _MAX_DEPTH:
        return '<too deep>'
    kw = {'project_dir': project_dir, '_depth': _depth + 1}
    t = type(value)
    hook = getattr(t, '__remake_hash__', None)
    if hook is not None and not isinstance(value, type):
        return f'{_qualname(t)}<{value.__remake_hash__()}>'
    if value is None or t in (bool, int, float, complex, str, bytes):
        return repr(value)
    if isinstance(value, enum.Enum):
        return f'{_qualname(t)}.{value.name}'
    if t is list:
        return '[' + ', '.join(canonical_value(v, **kw) for v in value) + ']'
    if t is tuple:
        inner = ', '.join(canonical_value(v, **kw) for v in value)
        return f'({inner},)' if len(value) == 1 else f'({inner})'
    if t is dict:
        items = (f'{canonical_value(k, **kw)}: {canonical_value(v, **kw)}'
                 for k, v in value.items())
        return '{' + _sorted_join(items) + '}'
    if t in (set, frozenset):
        if not value:
            return f'{t.__name__}()'
        inner = '{' + _sorted_join(canonical_value(v, **kw) for v in value) + '}'
        return inner if t is set else f'frozenset({inner})'
    if isinstance(value, types.ModuleType):
        return _module_rendering(value, project_dir)
    if isinstance(value, functools.partial):
        args = ', '.join(canonical_value(a, **kw) for a in value.args)
        kws = _sorted_join(f'{k}={canonical_value(v, **kw)}'
                           for k, v in value.keywords.items())
        return f'partial({canonical_value(value.func, **kw)}; {args}; {kws})'
    root = (getattr(t, '__module__', '') or '').split('.')[0]
    if root == 'numpy':
        rendered = _numpy_rendering(value, kw)
        if rendered is not None:
            return rendered
    if root == 'pandas':
        rendered = _pandas_rendering(value)
        if rendered is not None:
            return rendered
    if (inspect.isfunction(value) or inspect.ismethod(value) or inspect.isclass(value)
            or inspect.isbuiltin(value) or inspect.isroutine(value)):
        return _callable_rendering(value, kw)
    # Containers that aren't the exact builtins (dataclasses, namedtuples,
    # dict/list/set subclasses) render through their contents too, or a set
    # inside one would order randomly per process (code review 2026-11-21).
    name = _qualname(t)
    if dataclasses.is_dataclass(value):
        missing = object()  # an unset field(init=False) has no attribute
        fields = {f.name: v for f in dataclasses.fields(value)
                  if (v := getattr(value, f.name, missing)) is not missing}
        return f'{name}{canonical_value(fields, **kw)}'
    if isinstance(value, tuple) and hasattr(value, '_asdict'):
        return f'{name}{canonical_value(dict(value._asdict()), **kw)}'
    if isinstance(value, dict):
        return f'{name}{canonical_value(dict(value), **kw)}'
    if isinstance(value, (set, frozenset)):
        return f'{name}{canonical_value(set(value), **kw)}'
    if isinstance(value, (list, tuple)):
        return f'{name}{canonical_value(list(value), **kw)}'
    text = repr(value)
    if _has_address(text):
        # The repr embeds a memory address: it would differ every process.
        # Render the instance by its attributes when it has them; otherwise
        # all we can stably say is its type (body changes go unseen).
        state = getattr(value, '__dict__', None)
        if isinstance(state, dict):
            return f'{_qualname(t)}{canonical_value(state, **kw)}'
        return f'<unrenderable {_qualname(t)}>'
    return text


def _callable_rendering(fn, kw=None):
    if inspect.ismethod(fn):
        # Pass the depth on: an object holding a bound method of itself
        # would otherwise recurse without limit.
        return f'method {_qualname(fn)} of {canonical_value(fn.__self__, **(kw or {}))}'
    src = callable_source_ast(fn)
    if src is not None:
        return f'source {src}'
    code = getattr(fn, '__code__', None)
    if code is not None:
        return f'code {_qualname(fn)} {_code_digest(code)}'
    if inspect.isclass(fn):
        # Sourceless class (exec/REPL): only its name is stable; body
        # changes go unseen.
        return f'class {_qualname(fn)}'
    return f'builtin {_qualname(fn)}'


def _numpy_rendering(value, kw):
    import numpy as np

    if isinstance(value, np.ndarray):
        if value.dtype.hasobject:
            # Object arrays hold pointers: hash their elements instead.
            return (f'ndarray(object, {value.shape}, '
                    f'{canonical_value(value.tolist(), **kw)})')
        digest = hashlib.sha256(np.ascontiguousarray(value).tobytes()).hexdigest()
        return f'ndarray({value.dtype.str}, {value.shape}, {digest})'
    if isinstance(value, np.generic):
        return f'numpy.{type(value).__name__}({value.item()!r})'
    if isinstance(value, np.ufunc):
        return f'numpy.ufunc {value.__name__}'
    if isinstance(value, np.dtype):
        return f'numpy.dtype({value.str})'
    return None


def _pandas_rendering(value):
    import pandas as pd

    if isinstance(value, (pd.DataFrame, pd.Series, pd.Index)):
        try:
            hashed = pd.util.hash_pandas_object(value, index=not isinstance(value, pd.Index))
            digest = hashlib.sha256(hashed.values.tobytes()).hexdigest()
        except TypeError:
            # Unhashable object cells (lists, dicts): render the contents
            # instead (ultrareview 2026-11-21).
            if isinstance(value, pd.DataFrame):
                data = value.to_dict(orient='split')
            elif isinstance(value, pd.Series):
                data = {'index': list(value.index), 'data': list(value)}
            else:
                data = list(value)
            digest = hashlib.sha256(canonical_value(data).encode()).hexdigest()
        if isinstance(value, pd.DataFrame):
            meta = repr([(str(c), str(d)) for c, d in value.dtypes.items()])
        else:
            meta = repr((str(getattr(value, 'name', None)), str(value.dtype)))
        return f'{type(value).__name__}({meta}, {value.shape}, {digest})'
    return None


_ENV_PATHS = None


def _environment_roots():
    """Directories whose modules are environment (stdlib, site-packages)."""
    global _ENV_PATHS
    if _ENV_PATHS is None:
        import site
        import sysconfig

        roots = set()
        for key in ('stdlib', 'platstdlib', 'purelib', 'platlib'):
            p = sysconfig.get_paths().get(key)
            if p:
                roots.add(os.path.realpath(p))
        try:
            roots.update(os.path.realpath(p) for p in site.getsitepackages())
        except AttributeError:
            pass
        user = getattr(site, 'getusersitepackages', lambda: None)()
        if user:
            roots.add(os.path.realpath(user))
        roots.add(os.path.realpath(sys.prefix))
        roots.add(os.path.realpath(sys.base_prefix))
        _ENV_PATHS = tuple(sorted(roots))
    return _ENV_PATHS


def is_project_module(module, project_dir):
    """A module whose file lives under `project_dir` (the remakefile's
    directory) and not in the interpreter's environment: project code, whose
    edits can change results. Editable installs and PYTHONPATH checkouts
    elsewhere count as environment, like any installed library."""
    path = getattr(module, '__file__', None)
    if not path or not project_dir:
        return False
    path = os.path.realpath(path)
    if any(path.startswith(root + os.sep) for root in _environment_roots()):
        return False
    project = os.path.realpath(project_dir)
    return path.startswith(project + os.sep)


def _module_rendering(module, project_dir):
    if project_dir and is_project_module(module, project_dir):
        try:
            with open(module.__file__, 'rb') as f:
                tree = ast.parse(f.read())
            return f'module {module.__name__} {dump_ast(tree)}'
        except (OSError, SyntaxError, ValueError):
            pass
    return f'module {module.__name__}'


# --- uses ----------------------------------------------------------------------

def uses_parts(uses, project_dir=None):
    """{name: rendering} for a uses dict, name-sorted."""
    return {name: canonical_value(uses[name], project_dir=project_dir)
            for name in sorted(uses)}


def uses_hash(uses, project_dir=None):
    """The stored text for a uses dict: the marker line, then JSON of
    {name: rendering}. Empty uses is '' (as before 0.9.0, so equal)."""
    if not uses:
        return ''
    return FORMAT_MARKER + '\n' + json.dumps(uses_parts(uses, project_dir),
                                             sort_keys=True, ensure_ascii=False)


def is_canonical(stored):
    return stored.startswith(FORMAT_MARKER + '\n')


def parse_canonical(stored):
    return json.loads(stored[len(FORMAT_MARKER) + 1:])


# --- inputs/outputs ------------------------------------------------------------

def _sorted_dict_ast(tree):
    """Sort a dict display's keys (evaluated dict specs only: inside a
    callable's source, `{**a, 'k': v}` order is semantic)."""
    expr = tree.body[0].value if (tree.body and isinstance(tree.body[0], ast.Expr)) else None
    if isinstance(expr, ast.Dict) and all(k is not None for k in expr.keys):
        pairs = sorted(zip(expr.keys, expr.values), key=lambda kv: dump_ast(kv[0]))
        expr.keys = [k for k, _ in pairs]
        expr.values = [v for _, v in pairs]
    return tree


def io_part(spec):
    """Canonical text for one inputs/outputs spec: '' for none; a dict spec
    by its repr's AST with keys sorted (reordering keys is not a rerun
    trigger, matching `==`); a callable by its source AST."""
    if spec is None:
        return ''
    if callable(spec) and not isinstance(spec, dict):
        src = callable_source_ast(spec)
        if src is not None:
            return 'callable ' + src
        return 'callable ' + _callable_rendering(spec)
    from .tokens import FileToken

    # A FileToken is what a plain path string becomes anyway, and Ensure
    # options are not identity: `Ensure('a.nc')` and `'a.nc'` hash the same.
    def plain(v):
        return v.path if type(v) is FileToken else v

    spec = {k: (type(v)(plain(e) for e in v) if isinstance(v, (list, tuple)) else plain(v))
            for k, v in spec.items()}
    try:
        tree = ast.parse(repr(spec))
    except SyntaxError:
        return 'dict ' + canonical_value(spec)
    return 'dict ' + dump_ast(_sorted_dict_ast(tree))


def io_hash(rule):
    return FORMAT_MARKER + '\n' + json.dumps(
        {'inputs': io_part(rule.inputs), 'outputs': io_part(rule.outputs)},
        sort_keys=True, ensure_ascii=False)


# --- legacy (remake <= 0.9.0a1) ----------------------------------------------

def normalise_legacy_dump(text, *, dict_spec):
    """Canonical text for a legacy `ast.dump` segment (an io spec or a uses
    function), or None if it can't be rebuilt."""
    tree = ast_from_dump(text)
    if not isinstance(tree, ast.Module):
        return None
    if dict_spec:
        expr = tree.body[0].value if (tree.body and isinstance(tree.body[0], ast.Expr)) else None
        if isinstance(expr, ast.Dict):
            return 'dict ' + dump_ast(_sorted_dict_ast(tree))
        return None
    return dump_ast(normalise_def(tree))


def _literal(node):
    """ast.literal_eval, plus `set()` and `frozenset({...})`."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id in ('set', 'frozenset') and not node.keywords
            and len(node.args) <= 1):
        inner = _literal(node.args[0]) if node.args else ()
        return set(inner) if node.func.id == 'set' else frozenset(inner)
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        items = [_literal(e) for e in node.elts]
        return {ast.Set: set, ast.List: list, ast.Tuple: tuple}[type(node)](items)
    if isinstance(node, ast.Dict):
        return {_literal(k): _literal(v) for k, v in zip(node.keys, node.values)}
    return ast.literal_eval(node)


def normalise_legacy_value(text):
    """Canonical text for a legacy `repr` rendering of a plain value: parsed
    back when it is a literal (so a set's random order or a dict's key order
    stops mattering); otherwise the repr itself. (A legacy dataclass or
    namedtuple therefore doesn't match here; it matches on the exact repr
    comparison tried first, and is then restamped.)"""
    try:
        node = ast.parse(text, mode='eval').body
        return canonical_value(_literal(node))
    except (SyntaxError, ValueError, TypeError, RecursionError, MemoryError):
        return text
