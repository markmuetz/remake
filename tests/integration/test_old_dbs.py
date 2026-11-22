"""The current code opens the `.remake/remake.db` written by older releases.

The fixtures (tests/fixtures/old_dbs/<version>/, rebuilt by make_fixtures.py)
hold the DB, the remakefile and the outputs of one tiny pipeline run by
remake 0.8.5, 0.9.0 and 0.10.0. Opening one must migrate the schema in
place and plan nothing to rerun. That the previous release can still read
the migrated DB needs PyPI, so it is not tested here (the CI `upgrade` job,
tests/upgrade/check_upgrade.py, covers the upgrade direction on real runs).
"""
import shutil
import sqlite3
from pathlib import Path

import pytest

from remake import load_remake
from remake.metadata.sqlite3_backend import SCHEMA_VERSION

FIXTURES = Path(__file__).parent.parent / 'fixtures' / 'old_dbs'
# Release -> the PRAGMA user_version its DB carries (0 = unversioned, 0.8).
VERSIONS = {'0.8.5': 0, '0.9.0': 1000, '0.10.0': 1001}


def _user_version(db):
    """PRAGMA user_version of a DB file, read without migrating it."""
    conn = sqlite3.connect(db)
    try:
        return conn.execute('PRAGMA user_version').fetchone()[0]
    finally:
        conn.close()


@pytest.fixture(params=list(VERSIONS))
def old_project(request, tmp_path, monkeypatch):
    """(version, dir): a copy of one fixture in tmp_path, cwd inside it."""
    dest = tmp_path / 'project'
    shutil.copytree(FIXTURES / request.param, dest)
    monkeypatch.chdir(dest)
    return request.param, dest


def test_old_db_migrates_and_plans_nothing(old_project):
    version, old_project = old_project
    """Opening migrates the schema, and nothing would rerun."""
    db = old_project / '.remake' / 'remake.db'
    assert _user_version(db) == VERSIONS[version]
    rmk = load_remake(old_project / 'pipeline.py')
    try:
        runnable, deferred = rmk.plan()
        assert (runnable, deferred) == ([], [])
        assert _user_version(db) == SCHEMA_VERSION
    finally:
        rmk.metadata.close()


def test_old_db_info_works(old_project):
    _, old_project = old_project
    """`info` counts every task as up to date."""
    rmk = load_remake(old_project / 'pipeline.py')
    try:
        info = rmk.info()
    finally:
        rmk.metadata.close()
    totals = info['totals']
    assert totals['tasks'] == 4
    assert totals['up_to_date'] == 4
    assert {r['rule'] for r in info['rules']} == {'fx_make', 'fx_total'}
