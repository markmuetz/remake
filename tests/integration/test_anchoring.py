"""A Remake anchors relative paths at its defining file's directory (0.10 D6)."""
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from remake import Remake, rule

pytestmark = pytest.mark.real_anchoring

SCRIPT = textwrap.dedent('''
    from pathlib import Path
    from remake import Remake, rule

    @rule(outputs={'o': 'data/out.txt'})
    def make(outputs):
        Path(outputs['o']).write_text('x')

    rmk = Remake(rules=[make])
    assert rmk.run() == 0
    print(rmk.project_dir)
''')


def test_script_run_from_elsewhere_writes_beside_the_script(tmp_path):
    project = tmp_path / 'project'
    project.mkdir()
    (project / 'pipeline.py').write_text(SCRIPT)
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.mkdir()
    out = subprocess.run([sys.executable, str(project / 'pipeline.py')], cwd=elsewhere,
                         capture_output=True, text=True, check=True).stdout
    assert Path(out.strip()) == project.resolve()
    assert (project / 'data/out.txt').exists()
    assert (project / '.remake/remake.db').exists()
    assert not any(elsewhere.iterdir())


def test_no_defining_file_falls_back_to_the_cwd(tmp_path):
    out = subprocess.run([sys.executable, '-c', SCRIPT], cwd=tmp_path,
                         capture_output=True, text=True, check=True).stdout
    assert Path(out.strip()) == tmp_path.resolve()
    assert (tmp_path / 'data/out.txt').exists()


def test_explicit_project_dir_wins_and_cwd_is_restored(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / 'target'
    target.mkdir()

    @rule(outputs={'o': 'out.txt'})
    def make(outputs):
        Path(outputs['o']).write_text('x')

    rmk = Remake(rules=[make], project_dir=target)
    assert rmk.run() == 0
    assert (target / 'out.txt').exists() and not (tmp_path / 'out.txt').exists()
    assert Path.cwd() == tmp_path


def test_default_in_a_test_function_is_this_file():
    assert Remake().project_dir == str(Path(__file__).parent)
