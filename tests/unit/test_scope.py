import warnings

import pytest

from remake import Remake, ScopeError, ScopeWarning, Sqlite3Backend, rule
from remake.core.scope import exec_function, function_source, undeclared_names, uses_hash

CONSTANT = 42


def helper(x):
    return x + 1


def test_stdlib_and_builtin_names_are_safe():
    from pathlib import Path

    def fn():
        return Path(str(len('x')))

    assert undeclared_names(fn, {}) == []


def test_module_globals_are_safe():
    import json

    def fn():
        return json.dumps({})

    assert undeclared_names(fn, {}) == []


def test_constant_is_flagged():
    def fn():
        return CONSTANT

    assert undeclared_names(fn, {}) == ['CONSTANT']


def test_helper_function_is_flagged():
    def fn():
        return helper(1)

    assert undeclared_names(fn, {}) == ['helper']


def test_uses_declaration_silences():
    def fn():
        return CONSTANT + helper(1)

    assert undeclared_names(fn, {'CONSTANT': CONSTANT, 'helper': helper}) == []


def test_closure_variable_is_flagged():
    captured = 1

    def fn():
        return captured

    assert undeclared_names(fn, {}) == ['captured']


def test_decoration_warns():
    with pytest.warns(ScopeWarning, match='CONSTANT'):

        @rule(outputs={'o': 'o.txt'})
        def r(outputs):
            return CONSTANT


def _helper_v1(x):
    return x + 1


def test_uses_shadowing_different_object_warns():
    # uses={'helper': <other fn>} in a module that defines `helper`: inside
    # the rule the uses value wins, silently shadowing what the reader sees.
    with pytest.warns(ScopeWarning, match='shadow.*helper'):

        @rule(outputs={'o': 'o.txt'}, uses={'helper': _helper_v1})
        def r(outputs):
            return helper(1)


def test_uses_shadowing_same_object_is_silent():
    # The standard tracking idiom: declaring the module global itself.
    with warnings.catch_warnings():
        warnings.simplefilter('error', ScopeWarning)

        @rule(outputs={'o': 'o.txt'}, uses={'helper': helper, 'CONSTANT': CONSTANT})
        def r(outputs):
            return helper(CONSTANT)


def test_uses_shadowing_equal_value_is_silent():
    # A re-typed literal equal to the module global: harmless, no warning.
    with warnings.catch_warnings():
        warnings.simplefilter('error', ScopeWarning)

        @rule(outputs={'o': 'o.txt'}, uses={'CONSTANT': 42})
        def r(outputs):
            return CONSTANT


def test_rule_level_strict_raises_at_decoration():
    with pytest.raises(ScopeError, match='CONSTANT'):

        @rule(outputs={'o': 'o.txt'}, strict_scope=True)
        def r(outputs):
            return CONSTANT


def test_remake_level_strict_raises_at_registration():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', ScopeWarning)

        @rule(outputs={'o': 'o.txt'})
        def r(outputs):
            return CONSTANT

    with pytest.raises(ScopeError, match='CONSTANT'):
        Remake(strict_scope=True, rules=[r], metadata=Sqlite3Backend(':memory:'))


# --- uses_hash ---


def test_uses_hash_value_change():
    assert uses_hash({'t': 0.5}) != uses_hash({'t': 0.7})
    assert uses_hash({'t': 0.5}) == uses_hash({'t': 0.5})


def test_uses_hash_function_body_change(tmp_path):
    from remake.loader import load_module

    (tmp_path / 'v1.py').write_text('def normalise(x):\n    return x * 3\n')
    (tmp_path / 'v2.py').write_text(
        'def normalise(x):\n    # cosmetic change only\n    return x  *  3\n'
    )
    (tmp_path / 'v3.py').write_text('def normalise(x):\n    return x * 4\n')
    v1, v2, v3 = (load_module(tmp_path / f'v{i}.py').normalise for i in (1, 2, 3))

    # Cosmetic (comments/whitespace) changes do not change the hash.
    assert uses_hash({'normalise': v1}) == uses_hash({'normalise': v2})
    # Body changes do.
    assert uses_hash({'normalise': v1}) != uses_hash({'normalise': v3})


def test_uses_hash_class_body_change(tmp_path):
    """uses can take a class: hashed like a function (whole class body,
    AST-normalised), injected like any value."""
    from remake.loader import load_module

    body = 'class Calib:\n    OFFSET = {o}\n    def apply(self, x):\n        return x + self.OFFSET\n'
    (tmp_path / 'c1.py').write_text(body.format(o=1.5))
    (tmp_path / 'c2.py').write_text(body.format(o=1.5).replace('    def', '\n    # comment\n    def'))
    (tmp_path / 'c3.py').write_text(body.format(o=2.5))
    c1, c2, c3 = (load_module(tmp_path / f'c{i}.py').Calib for i in (1, 2, 3))

    assert uses_hash({'Calib': c1}) == uses_hash({'Calib': c2})  # cosmetic
    assert uses_hash({'Calib': c1}) != uses_hash({'Calib': c3})  # body change

    def fn(x):
        return Calib().apply(x)  # noqa: F821 — injected via uses

    assert exec_function(fn, {'Calib': c1})(1.0) == 2.5


def test_uses_hash_handles_sourceless_class():
    # A class defined via exec has no source AND no __code__: must not
    # crash; falls back to repr (body changes undetected, documented).
    ns = {}
    exec('class C:\n    pass\n', ns)
    h = uses_hash({'C': ns['C']})
    assert 'unsourced:' in h


def test_uses_hash_handles_sourceless_functions():
    # Functions defined via exec/REPL have no retrievable source.
    ns = {}
    exec('def f(x):\n    return x * 2\n', ns)
    h = uses_hash({'f': ns['f']})
    assert 'bytecode:' in h
    assert function_source(ns['f']).startswith("'<bytecode:")


# --- exec_function (uses injection) ---


def test_exec_function_injects_uses_as_globals():
    def fn(x):
        return x * scale  # noqa: F821 — injected via uses

    injected = exec_function(fn, {'scale': 10})
    assert injected(3) == 30


def test_exec_function_without_uses_is_identity():
    def fn():
        return 1

    assert exec_function(fn, {}) is fn
