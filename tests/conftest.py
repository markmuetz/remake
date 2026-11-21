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
