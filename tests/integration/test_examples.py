"""Every dependency-light example loads, registers and plans with the
expected task counts. (ex6 needs numpy/pandas/xarray at import time and is
exercised only if they are installed.)"""
from pathlib import Path

import pytest

from remake import Sqlite3Backend, load_remake

EXAMPLES_DIR = Path(__file__).parent.parent.parent / 'examples'

CASES = [
    ('ex1_simple.py', 2, 2),
    ('ex2_matrix.py', 4, 70),
    ('ex3_uses_scope.py', 2, 11),
    ('ex4_zarr_slurm.py', 4, 357),
    ('ex5_callable_inputs_matrix.py', 4, 55),
    ('ex7_multifile/pipeline.py', 3, 69),
]


@pytest.mark.filterwarnings('ignore::remake.ScopeWarning')
@pytest.mark.parametrize('filename,n_rules,n_tasks', CASES)
def test_example_loads_and_plans(filename, n_rules, n_tasks, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # examples use relative paths
    rmk = load_remake(EXAMPLES_DIR / filename, finalize=False)
    rmk.metadata = Sqlite3Backend(':memory:')
    runnable, deferred = rmk.plan()
    assert len(rmk.rules) == n_rules
    assert len(runnable) == n_tasks
    assert not deferred


@pytest.mark.filterwarnings('ignore::remake.ScopeWarning')
def test_ex6_zarr_region():
    pytest.importorskip('xarray')
    pytest.importorskip('pandas')
    rmk = load_remake(EXAMPLES_DIR / 'ex6_zarr_region.py', finalize=False)
    rmk.metadata = Sqlite3Backend(':memory:')
    runnable, deferred = rmk.plan()
    assert len(rmk.rules) == 3
    assert len(runnable) == 46  # 1 + 44 years + 1
    assert not deferred
