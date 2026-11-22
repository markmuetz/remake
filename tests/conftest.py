import os

import pytest

from remake import Sqlite3Backend


@pytest.fixture
def meta():
    return Sqlite3Backend(':memory:')


@pytest.fixture(autouse=True)
def _isolated_user_config(tmp_path_factory, monkeypatch):
    """Never read the real user's remake config (or profile selection)."""
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path_factory.mktemp('xdg')))
    monkeypatch.delenv('REMAKE_PROFILE', raising=False)


@pytest.fixture(autouse=True)
def _anchor_test_remakes_at_cwd(request, monkeypatch):
    """A Remake built in a test function would anchor at the test file's
    directory (0.10 D6) and write into tests/. Tests chdir to tmp_path and
    expect that, so anchor at the cwd instead, except in tests marked
    `real_anchoring`, which check the real behaviour."""
    if request.node.get_closest_marker('real_anchoring') is None:
        monkeypatch.setattr('remake.core.remake._default_project_dir', os.getcwd)
