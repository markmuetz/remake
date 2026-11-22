"""The SLURM submit script records the submitting Python environment."""
import sys
from pathlib import Path

from remake.remake_cmd import remake_cmd

PIPELINE = '''
import localhelper
from pathlib import Path
from remake import Remake, rule

@rule(outputs={'o': 'data/e.txt'})
def e(outputs):
    Path(outputs['o']).write_text(str(localhelper.X))

rmk = Remake()
rmk.rules_from_current_module()
'''


def test_submit_sh_header_records_environment(tmp_path, monkeypatch):
    """A wrong interpreter or a wrong checkout of an imported package must
    be visible after the fact, in the submit.sh header."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('PYTHONPATH', '/some/dev/checkout')
    Path('localhelper.py').write_text('X = 1\n')
    Path('envpipe.py').write_text(PIPELINE)
    remake_cmd(['remake', 'run', 'envpipe.py', '-E', 'slurm', '--dry-run'])

    header = [line for line in Path('.remake/submit.sh').read_text().splitlines()
              if line.startswith('#')]
    text = '\n'.join(header)
    assert f'sys.executable: {sys.executable}' in text
    assert 'sys.version: ' in text
    assert 'PYTHONPATH: /some/dev/checkout' in text
    assert 'sys.path: [' in text
    assert 'environment fingerprint: ' in text
    assert 'remake: ' in text
    assert f'module localhelper: {Path("localhelper.py").resolve()}' in text
    # stdlib and site-packages modules are not listed
    assert 'module json:' not in text
    assert 'module loguru:' not in text
