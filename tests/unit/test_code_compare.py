from remake.util.code_compare import CodeComparer


def test_identical_source_is_equal():
    cc = CodeComparer()
    assert cc('x = 1\n', 'x = 1\n')


def test_cosmetic_difference_is_equal():
    # Comments / blank lines / formatting don't count — AST-normalised compare.
    cc = CodeComparer()
    assert cc('def f():\n    return 1\n',
              'def f():\n    # a comment\n\n    return  1\n')


def test_meaningful_difference_is_not_equal():
    cc = CodeComparer()
    assert not cc('def f():\n    return 1\n', 'def f():\n    return 2\n')


def test_unparseable_source_degrades_to_changed():
    # SyntaxError → "changed" rather than raising.
    cc = CodeComparer()
    assert not cc('def f(:\n', 'def f():\n    return 1\n')


def test_null_byte_source_degrades_to_changed():
    # ast.parse raises ValueError (not SyntaxError) on null bytes; the defensive
    # net must treat it as changed, not crash the plan (intent of remake2's
    # cdb2e98, ported cleanly). The two strings differ, so rerun is safe.
    cc = CodeComparer()
    assert not cc('x = 1\x00\n', 'x = 1\n')
