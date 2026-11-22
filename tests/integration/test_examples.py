"""Every dependency-light example loads, registers and plans with the
expected task counts. (ex9 needs numpy/pandas/xarray at import time and is
exercised only if they are installed.)

The examples are also held warning-clean, on both channels: Python warnings
are errors (except ScopeWarning, which ex3 deliberately demonstrates), and
anything logged at WARNING+ through loguru (e.g. the duplicate-rule-name
collision warning) fails the test via the autouse fixture below.
"""
import shutil
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
    try:
        logger.remove(sink_id)
    except ValueError:  # the CLI resets loguru's handlers (tests that call it)
        pass
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
    ('ex13_rule_factory.py', 5, 11),
    ('ex15_config/pipeline.py', 2, 4),
    ('ex16_diagnostics.py', 3, 3),
    ('ex17_list_values.py', 3, 7),
]


@pytest.fixture
def ex_dir(tmp_path, monkeypatch):
    """A copy of examples/ (sources only), cwd inside it. remake anchors
    paths at a remakefile's directory (0.10 D6), so running the originals
    would write into the repo's examples/."""
    dest = tmp_path / 'examples'
    shutil.copytree(EXAMPLES_DIR, dest, ignore=shutil.ignore_patterns(
        'data', '.remake', '__pycache__', 'validated.yaml'))
    monkeypatch.chdir(dest)
    return dest


@pytest.mark.parametrize('filename,n_rules,n_tasks', CASES)
def test_example_loads_and_plans(filename, n_rules, n_tasks, ex_dir, memory_backend):
    rmk = load_remake(ex_dir / filename, finalize=False)
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
    ('ex13', 'data/results/summary13.txt'),         # self-generating
    ('ex14', 'data/results/summary14.txt'),         # self-generating
    ('ex16', 'data/results/report16.txt'),
    ('ex17', 'data/ex17/summary17.txt'),          # self-generating
])
def test_light_examples_run_end_to_end(example, result_file, ex_dir, memory_backend):
    """These need no heavy deps: generate synthetic data (if any) and run."""
    if example == 'ex4':
        pytest.importorskip('yaml')
    make_data = load_module(ex_dir / 'make_example_data.py')
    if hasattr(make_data, example):
        getattr(make_data, example)()

    filename = next(ex_dir.glob(f'{example}_*.py'))
    rmk = load_remake(filename, finalize=False)
    rmk.metadata = memory_backend
    rmk.run()
    assert (ex_dir / result_file).exists()
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_ex9_zarr_region(ex_dir, memory_backend):
    pytest.importorskip('xarray')
    pytest.importorskip('pandas')
    rmk = load_remake(ex_dir / 'ex9_zarr_region.py', finalize=False)
    rmk.metadata = memory_backend
    runnable, deferred = rmk.plan()
    assert len(rmk.rules) == 3
    assert len(runnable) == 46  # 1 + 44 years + 1
    assert not deferred


def test_ex15_config_runs_and_resolves_config(ex_dir, tmp_path, monkeypatch, memory_backend):
    """The directory example runs, and its remake.toml layers are applied."""
    monkeypatch.delenv("REMAKE_PROFILE", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "no_user_config"))
    monkeypatch.chdir(ex_dir / "ex15_config")
    rmk = load_remake(ex_dir / "ex15_config" / "pipeline.py", finalize=False)
    rmk.metadata = memory_backend
    assert rmk.config["checksum_outputs"] is True
    assert rmk.config["multiproc"]["nproc"] == 2  # project file
    assert rmk.config["resources"]["capture"] is True  # Remake(config=)
    assert rmk.config["resources"]["rss_interval"] == 0.5  # profile quick
    rmk.run()
    assert (ex_dir / "ex15_config/data/results/total15.txt").read_text() == "57\n"  # 6 + 15 + 36
    runnable, deferred = rmk.plan()
    assert not runnable and not deferred


def test_rule_names_are_unique_across_examples(ex_dir):
    """The top-level examples share one .remake/ store, so no rule name may
    appear in two files (the duplicate-rule warning would fire, and the two
    would clobber each other's recorded state). ex5_multifile/ and
    ex15_config/ are their own pipeline directories and are not included."""
    owners = {}
    for path in sorted(ex_dir.glob('ex*.py')):
        try:
            rmk = load_remake(path, finalize=False)
        except ImportError:  # e.g. ex9 without xarray
            continue
        for r in rmk.rules:
            owners.setdefault(r.name, []).append(path.name)
    clashes = {name: files for name, files in owners.items() if len(files) > 1}
    assert not clashes, f'rule names defined in more than one example: {clashes}'


def test_ex16_history_after_failing_and_fixing(tmp_path, monkeypatch):
    """The ex16 walk-through: a forced failure then a fix gives calibrate16
    three executions, and `history --json` and `--runs --json` show them."""
    import json

    from remake.remake_cmd import remake_cmd

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('REMAKE_EX16_FAIL', raising=False)
    getattr(load_module(EXAMPLES_DIR / 'make_example_data.py'), 'ex16')()
    # remake anchors paths to the remakefile's directory: run a copy.
    shutil.copy(EXAMPLES_DIR / 'ex16_diagnostics.py', tmp_path)
    pipeline = str(tmp_path / 'ex16_diagnostics.py')
    assert (remake_cmd(['remake', 'run', pipeline]) or 0) == 0
    monkeypatch.setenv('REMAKE_EX16_FAIL', 'raise')
    assert remake_cmd(['remake', 'run', '-f', pipeline]) == 1
    monkeypatch.delenv('REMAKE_EX16_FAIL')
    assert (remake_cmd(['remake', 'run', pipeline]) or 0) == 0

    import contextlib
    import io

    def history(*argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            assert (remake_cmd(['remake', 'history', pipeline, *argv, '--json']) or 0) == 0
        return json.loads(buf.getvalue())

    task = history('-Q', "rule == 'calibrate16'")
    assert task['format'] == 1
    assert [(e['status'], e['reason']) for e in task['executions']] == [
        ('success', 'last-run-failed'), ('failed', 'forced'), ('success', 'never-run')]
    assert task['executions'][1]['error'].startswith('RuntimeError at ex16_diagnostics.py')
    runs = history('--runs')['runs']
    assert [r['n_failed'] for r in runs] == [0, 1, 0]
