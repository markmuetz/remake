"""Every dependency-light example loads, registers and plans with the
expected task counts. (ex9 needs numpy/pandas/xarray at import time and is
exercised only if they are installed.)

The examples are also held warning-clean, on both channels: Python warnings
are errors (except ScopeWarning, which ex3 deliberately demonstrates), and
anything logged at WARNING+ through loguru (e.g. the duplicate-rule-name
collision warning) fails the test via the autouse fixture below.
"""
from pathlib import Path

import pytest
from loguru import logger

from remake import Sqlite3Backend, load_remake
from remake.loader import load_module

EXAMPLES_DIR = Path(__file__).parent.parent.parent / 'examples'

pytestmark = [
    pytest.mark.filterwarnings('error'),
    pytest.mark.filterwarnings('ignore::remake.ScopeWarning'),
]


@pytest.fixture(autouse=True)
def fail_on_logged_warnings():
    """Fail if anything logs at WARNING or above during the test."""
    records = []
    sink_id = logger.add(
        lambda message: records.append(message.record), level='WARNING')
    yield
    logger.remove(sink_id)
    assert not records, 'logged warnings during test: ' + '; '.join(
        f'{r["level"].name}: {r["message"]}' for r in records)


@pytest.fixture
def memory_backend():
    """An in-memory backend, closed deterministically at teardown."""
    backend = Sqlite3Backend(':memory:')
    yield backend
    backend.close()

CASES = [
    ('ex1_simple.py', 2, 2),
    ('ex2_matrix.py', 4, 70),
    ('ex3_uses_scope.py', 2, 11),
    ('ex4_callable_inputs.py', 4, 55),
    ('ex5_multifile/pipeline.py', 3, 69),
    ('ex6_tuple_matrix.py', 2, 27),
    ('ex7_orchestration_only.py', 3, 7),
    ('ex8_zarr_slurm.py', 4, 357),
    ('ex11_custom_token.py', 1, 3),
    ('ex12_chained_loop_rules.py', 6, 16),
]


@pytest.mark.parametrize('filename,n_rules,n_tasks', CASES)
def test_example_loads_and_plans(filename, n_rules, n_tasks, tmp_path, monkeypatch, memory_backend):
    monkeypatch.chdir(tmp_path)  # examples use relative paths
    rmk = load_remake(EXAMPLES_DIR / filename, finalize=False)
    rmk.metadata = memory_backend
    runnable, deferred = rmk.plan()
    assert len(rmk.rules) == n_rules
    assert len(runnable) == n_tasks
    assert not deferred


@pytest.mark.parametrize('example,result_file', [
    ('ex1', 'data/results/summary.txt'),
    ('ex3', 'data/results/all_years.csv'),
    ('ex4', 'data/annual/site_a/2020.csv'),
    ('ex7', 'data/orchestrated/summary.txt'),     # self-generating
    ('ex10', 'data/results/event_summary.json'),   # self-generating
    ('ex11', 'data/results.db'),                   # self-generating
    ('ex12', 'data/stage0/alpha.txt'),              # self-generating
])
def test_light_examples_run_end_to_end(example, result_file, tmp_path, monkeypatch, memory_backend):
    """These need no heavy deps: generate synthetic data (if any) and run."""
    if example == 'ex4':
        pytest.importorskip('yaml')
    monkeypatch.chdir(tmp_path)
    make_data = load_module(EXAMPLES_DIR / 'make_example_data.py')
    if hasattr(make_data, example):
        getattr(make_data, example)()

    filename = next(EXAMPLES_DIR.glob(f'{example}_*.py'))
    rmk = load_remake(filename, finalize=False)
    rmk.metadata = memory_backend
    rmk.run()
    assert (tmp_path / result_file).exists()
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_ex9_zarr_region(memory_backend):
    pytest.importorskip('xarray')
    pytest.importorskip('pandas')
    rmk = load_remake(EXAMPLES_DIR / 'ex9_zarr_region.py', finalize=False)
    rmk.metadata = memory_backend
    runnable, deferred = rmk.plan()
    assert len(rmk.rules) == 3
    assert len(runnable) == 46  # 1 + 44 years + 1
    assert not deferred
