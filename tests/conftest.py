import pytest

from remake import Sqlite3Backend


@pytest.fixture
def meta():
    return Sqlite3Backend(':memory:')
