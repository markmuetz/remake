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


def test_subclass_anchors_at_the_file_that_instantiates_it(tmp_path):
    # /code-review 2026-11-22 #8.
    lib, proj = tmp_path / 'lib', tmp_path / 'proj'
    lib.mkdir()
    proj.mkdir()
    (lib / 'mylib_r8.py').write_text(textwrap.dedent('''
        from remake import Remake

        class MyRemake(Remake):
            def __init__(self, **kw):
                super().__init__(**kw)
    '''))
    (proj / 'pipeline.py').write_text(textwrap.dedent(f'''
        import sys
        sys.path.insert(0, {str(lib)!r})
        from mylib_r8 import MyRemake
        print(MyRemake().project_dir)
    '''))
    out = subprocess.run([sys.executable, str(proj / 'pipeline.py')], cwd=tmp_path,
                         capture_output=True, text=True, check=True).stdout
    assert Path(out.strip()) == proj.resolve()
