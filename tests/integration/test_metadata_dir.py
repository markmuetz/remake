"""The `metadata_dir` config key (0.10 item 5f, D7): `.remake/` moved
elsewhere, for the CLI, workers and the SLURM executor."""
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from remake.remake_cmd import remake_cmd
from remake.util.paths import metadata_dir, set_metadata_dir

from test_slurm import SBATCH_SHIM, SQUEUE_SHIM, read_specs  # noqa: F401

PIPELINE = '''
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'raw': 'data/raw_{n}.txt'}, matrix={'n': [1, 2, 3]})
def generate(outputs, n):
    Path(outputs['raw']).write_text(str(n))

@rule(inputs=generate.outputs, outputs={'out': 'data/out_{n}.txt'},
      matrix=generate.matrix, depends_on=[generate])
def process(inputs, outputs, n):
    Path(outputs['out']).write_text(Path(inputs['raw']).read_text() * 2)

rmk = Remake(config={'slurm': {'partition': 'test-par'}})
rmk.rules_from_current_module()
'''


def cli(*args):
    return remake_cmd(['remake', *args]) or 0


def make_project(tmp_path, monkeypatch, toml='metadata_dir = "../meta"\n'):
    proj = tmp_path / 'proj'
    proj.mkdir()
    (proj / 'pipeline.py').write_text(PIPELINE)
    if toml:
        (proj / 'remake.toml').write_text(toml)
    monkeypatch.chdir(proj)
    return proj, tmp_path / 'meta'


def test_paths_default_and_set(tmp_path, monkeypatch):
    monkeypatch.setenv('MD_TEST', 'envdir')
    assert metadata_dir() == Path('.remake')
    assert set_metadata_dir('rel', base=tmp_path) == tmp_path / 'rel'
    assert set_metadata_dir('$MD_TEST/x', base=tmp_path) == tmp_path / 'envdir' / 'x'
    assert set_metadata_dir('~/m').is_absolute()
    set_metadata_dir(None)
    assert metadata_dir() == Path('.remake')


def test_metadata_dir_must_be_a_string(tmp_path, monkeypatch, capsys):
    make_project(tmp_path, monkeypatch, 'metadata_dir = 3\n')
    assert cli('run', 'pipeline.py') == 2
    assert 'metadata_dir' in capsys.readouterr().err


def test_metadata_dir_not_allowed_in_user_file(tmp_path, monkeypatch, capsys):
    proj, _ = make_project(tmp_path, monkeypatch, toml=None)
    user = Path(os.environ['XDG_CONFIG_HOME']) / 'remake' / 'config.toml'
    user.parent.mkdir(parents=True)
    user.write_text('metadata_dir = "/x"\n')
    assert cli('run', 'pipeline.py') == 2
    assert 'metadata_dir' in capsys.readouterr().err


@pytest.mark.parametrize('executor', ['singleproc', 'multiproc'])
def test_run_info_history_use_metadata_dir(tmp_path, monkeypatch, capsys, executor):
    proj, meta = make_project(tmp_path, monkeypatch)
    assert cli('run', 'pipeline.py', '-E', executor) == 0
    assert (proj / 'data' / 'out_3.txt').read_text() == '33'
    assert (meta / 'remake.db').exists()
    assert (meta / 'remake.log').exists()
    if executor == 'multiproc':
        assert list((meta / 'tasks' / 'log').rglob('*.log'))
    assert not (proj / '.remake').exists()
    capsys.readouterr()

    # Reading commands find it again, and a second run has nothing to do.
    assert cli('info', 'pipeline.py') == 0
    assert re.search(r'TOTAL\s+6\s+6', capsys.readouterr().out)
    assert cli('run', 'pipeline.py', '-E', executor, '--dry-run') == 0
    assert '0 task(s) would run' in capsys.readouterr().out
    capsys.readouterr()
    assert cli('task-info', 'pipeline.py', '-Q', 'rule == "generate" and n == 1') == 0
    assert 'generate' in capsys.readouterr().out
    assert cli('history', 'pipeline.py', '--runs') == 0
    assert capsys.readouterr().out.strip()
    assert cli('history', 'pipeline.py', '-Q', 'rule == "generate" and n == 1') == 0
    capsys.readouterr()
    with closing(sqlite3.connect(meta / 'remake.db')) as conn:
        assert conn.execute('select count(*) from task').fetchone()[0] == 6
    assert not (proj / '.remake').exists()


def test_absolute_dir_and_programmatic_config(tmp_path, monkeypatch):
    proj, _ = make_project(tmp_path, monkeypatch, toml=None)
    abs_meta = tmp_path / 'elsewhere' / 'md'
    src = PIPELINE.replace("config={'slurm'", f"config={{'metadata_dir': {str(abs_meta)!r}, 'slurm'")
    (proj / 'pipeline.py').write_text(src)
    assert cli('run', 'pipeline.py') == 0
    assert (abs_meta / 'remake.db').exists()
    # Only the early CLI log files can land in .remake/ (documented): the
    # database and everything else are in the configured directory.
    assert not (proj / '.remake' / 'remake.db').exists()


def test_no_key_keeps_dot_remake(tmp_path, monkeypatch):
    proj, meta = make_project(tmp_path, monkeypatch, toml=None)
    assert cli('run', 'pipeline.py') == 0
    assert (proj / '.remake' / 'remake.db').exists()
    assert not meta.exists()


def test_setting_does_not_leak_between_remakes(tmp_path, monkeypatch):
    proj, meta = make_project(tmp_path, monkeypatch)
    assert cli('run', 'pipeline.py') == 0
    (proj / 'remake.toml').unlink()
    (proj / 'data' / 'out_1.txt').unlink()
    assert cli('run', 'pipeline.py') == 0
    assert (proj / '.remake' / 'remake.db').exists()


@pytest.fixture
def slurm_project(tmp_path, monkeypatch):
    proj, meta = make_project(tmp_path, monkeypatch)
    shim = tmp_path / 'shim'
    shim.mkdir()
    for name, text in [('sbatch', SBATCH_SHIM), ('squeue', SQUEUE_SHIM)]:
        (shim / name).write_text(text)
        (shim / name).chmod(0o755)
    monkeypatch.setenv('PATH', f'{shim}{os.pathsep}{os.environ["PATH"]}')
    return proj, meta, shim


def test_slurm_files_land_in_metadata_dir(slurm_project):
    proj, meta, shim = slurm_project
    assert cli('run', 'pipeline.py', '-E', 'slurm') == 0
    assert not (proj / '.remake').exists()
    assert (meta / 'submit.sh').exists()
    assert (meta / 'slurm' / 'generate.sbatch').exists()
    assert (meta / 'slurm' / 'output' / 'generate').is_dir()
    assert list((meta / 'jobs').glob('generate.*.json'))
    assert (meta / 'jobs' / 'generate.jobids.json').exists()
    sbatch = (meta / 'slurm' / 'generate.sbatch').read_text()
    assert str(meta / 'slurm' / 'output' / 'generate') in sbatch
    assert str(meta / 'jobs') in sbatch  # --specs path
    submit = (meta / 'submit.sh').read_text()
    assert str(meta / 'slurm' / 'generate.sbatch') in submit
    assert str(meta / 'jobs' / 'generate.jobids.json') in submit

    # Array elements (fresh load_remake, as on a compute node) pick it up.
    n = len((shim / 'sbatch.log').read_text().splitlines())
    key = read_specs('generate')[1]['task_key']
    assert cli('run-array-task', 'pipeline.py', 'generate', '1') == 0
    assert (meta / 'tasks' / 'results' / 'generate' / key[:2] / f'{key[2:]}.json').exists()
    assert (meta / 'tasks' / 'log' / 'generate' / key[:2] / f'{key[2:]}.log').exists()
    assert not (proj / '.remake').exists()

    # resubmit re-executes the relocated submit.sh.
    assert cli('resubmit', 'pipeline.py') == 0
    assert len((shim / 'sbatch.log').read_text().splitlines()) == 2 * n


def test_unset_variable_in_metadata_dir_is_an_error(monkeypatch, tmp_path):
    # /code-review 2026-11-22 #5: '$SCRATCH/proj' with SCRATCH unset.
    from remake.core.exceptions import RemakeError
    from remake.util.paths import set_metadata_dir

    monkeypatch.delenv('REMAKE_TEST_UNSET', raising=False)
    with pytest.raises(RemakeError, match='not set'):
        set_metadata_dir('$REMAKE_TEST_UNSET/proj', base=tmp_path)
    monkeypatch.setenv('REMAKE_TEST_UNSET', str(tmp_path / 's'))
    assert set_metadata_dir('$REMAKE_TEST_UNSET/proj', base=tmp_path) == tmp_path / 's/proj'
