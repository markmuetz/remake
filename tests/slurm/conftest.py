"""Fixtures for the SLURM reconciliation tests: a project directory with
the simulated cluster's shims on PATH (fake_cluster.py)."""
import os
from pathlib import Path

import pytest

from .fake_cluster import FakeCluster

# chain: gen (4) -> proc (4, aftercorr) -> agg (1, afterok).
CHAIN = '''
from pathlib import Path
from remake import Remake, rule

N = 4

@rule(outputs={'o': 'data/gen_{n}.txt'}, matrix={'n': list(range(N))})
def gen(outputs, n):
    Path(outputs['o']).write_text(str(n))

@rule(inputs=gen.outputs, outputs={'o': 'data/proc_{n}.txt'},
      matrix=gen.matrix, depends_on=[gen])
def proc(inputs, outputs, n):
    Path(outputs['o']).write_text(Path(inputs['o']).read_text() * 2)

def agg_inputs():
    return {str(n): f'data/proc_{n}.txt' for n in range(N)}

@rule(inputs=agg_inputs, outputs={'o': 'data/agg.txt'}, depends_on=[proc])
def agg(inputs, outputs):
    Path(outputs['o']).write_text(','.join(Path(p).read_text() for p in inputs.values()))

rmk = Remake(config={'slurm': {'partition': 'test-par'}})
rmk.rules_from_current_module()
'''


def pytest_addoption(parser):
    parser.addoption('--live-slurm', action='store_true', default=False,
                     help='run tests/slurm/live on a real SLURM cluster')


def pytest_collection_modifyitems(config, items):
    if config.getoption('--live-slurm'):
        return
    skip = pytest.mark.skip(reason='live SLURM test: pass --live-slurm on a cluster')
    for item in items:
        if 'live_slurm' in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    """A project dir (cwd) with the CHAIN remakefile as pipeline.py and the
    simulated cluster on PATH."""
    monkeypatch.chdir(tmp_path)
    fake = FakeCluster(tmp_path)
    monkeypatch.setenv('PATH', f'{fake.bin}{os.pathsep}{os.environ["PATH"]}')
    Path('pipeline.py').write_text(CHAIN)
    return fake
