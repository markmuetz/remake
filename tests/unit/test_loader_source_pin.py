"""Editing a remakefile after it is loaded must not change the source remake
records for its functions (a long SLURM task records it when it finishes)."""
import inspect
import os
import sys

from remake.core.canonical import callable_source_ast
from remake.loader import load_module

SRC = '''"""Doc."""


def helper(x):
    return x + 1


def target(x):
    return helper(x) * 2
'''


def test_source_pinned_after_edit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / 'pinned_mod.py'
    path.write_text(SRC)
    try:
        mod = load_module('pinned_mod.py')
        before = callable_source_ast(mod.target)
        helper_before = inspect.getsource(mod.helper)

        # Shift every function down two lines, as an edit to the docstring does.
        path.write_text(SRC.replace('"""Doc."""', '"""Doc.\n\nMore.\n"""'))
        st = path.stat()
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))

        assert callable_source_ast(mod.target) == before
        assert inspect.getsource(mod.helper) == helper_before
        assert 'def target' in inspect.getsource(mod.target)
    finally:
        sys.modules.pop('pinned_mod', None)
