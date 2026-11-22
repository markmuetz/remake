"""Rebuild the old-release DB fixtures in this directory.

    uv run python tests/fixtures/old_dbs/make_fixtures.py            # all
    uv run python tests/fixtures/old_dbs/make_fixtures.py 0.10.0     # one

For each version V this installs ``remake==V`` from PyPI into a throwaway uv
venv, runs ``pipeline.py`` (two rules, four tasks) in a temp dir with it, and
copies ``pipeline.py``, ``.remake/remake.db`` and the outputs in ``data/`` to
``V/``. Only ``remake.db`` is kept from ``.remake/`` (no logs or sidecars).
The test is tests/integration/test_old_dbs.py. Needs ``uv`` and network
access; the committed output is what the tests use, so this only needs
re-running to add a release (add it to VERSIONS and the test's list).
"""
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
VERSIONS = ['0.8.5', '0.9.0', '0.10.0']


def build(version):
    """Make the fixture for one release under HERE/<version>/."""
    with tempfile.TemporaryDirectory(prefix='remake-fixture-') as tmp:
        tmp = Path(tmp)
        venv = tmp / 'venv'
        subprocess.run(['uv', 'venv', '-q', '--python', '3.13', str(venv)], check=True)
        subprocess.run(['uv', 'pip', 'install', '-q', '--python', str(venv / 'bin' / 'python'),
                        f'remake=={version}'], check=True)
        work = tmp / 'work'
        work.mkdir()
        shutil.copy(HERE / 'pipeline.py', work)
        subprocess.run([str(venv / 'bin' / 'remake'), 'run', 'pipeline.py'], cwd=work, check=True)
        db = work / '.remake' / 'remake.db'
        conn = sqlite3.connect(db)  # fold any WAL into the main file
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        conn.close()
        out = HERE / version
        if out.exists():
            shutil.rmtree(out)
        (out / '.remake').mkdir(parents=True)
        shutil.copy(work / 'pipeline.py', out)
        shutil.copy(db, out / '.remake' / 'remake.db')
        shutil.copytree(work / 'data', out / 'data')
    print(f'wrote {out}')


def main(argv):
    """Build the requested versions (default: all)."""
    for version in argv[1:] or VERSIONS:
        build(version)


if __name__ == '__main__':
    main(sys.argv)
