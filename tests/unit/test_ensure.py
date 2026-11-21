"""0.9.0 item 4: output validation (Ensure) and opt-in checksum capture."""
import hashlib
from pathlib import Path

import pytest

from remake import Ensure, FileToken, Remake, Sqlite3Backend, rule
from remake.core.scope import io_hash
from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS


def _run(tmp_path, rules, **config):
    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=rules, metadata=db, config=config)
    return rmk, rmk.run(), db


def test_ensure_returns_the_same_token_kind_with_options():
    tok = Ensure('out/{x}.nc', min_size=10, checksum=True)
    assert type(tok) is FileToken
    formatted = tok.format(x=1)
    assert formatted.path == 'out/1.nc' and formatted.min_size == 10 and formatted.checksum


def test_failed_check_fails_the_task(tmp_path):
    @rule(outputs={'o': Ensure(str(tmp_path / 'o.txt'))})
    def empty(outputs):
        Path(outputs['o']).write_text('')

    rmk, nfailed, db = _run(tmp_path, [empty])
    assert nfailed == 1
    (task,) = rmk.tasks()
    rec = db.get_tasks_status([task])[task.key]
    assert rec.status == TASK_STATUS_FAILED and 'is empty' in rec.exception


def test_min_size(tmp_path):
    @rule(outputs={'o': Ensure(str(tmp_path / 'o.txt'), min_size=5)})
    def small(outputs):
        Path(outputs['o']).write_text('abc')

    _, nfailed, _ = _run(tmp_path, [small])
    assert nfailed == 1


def test_missing_output_fails_only_under_ensure(tmp_path):
    @rule(outputs={'o': str(tmp_path / 'plain.txt')})
    def plain(outputs):
        pass

    @rule(outputs={'o': Ensure(str(tmp_path / 'ensured.txt'))})
    def ensured(outputs):
        pass

    _, nfailed, _ = _run(tmp_path, [plain, ensured])
    assert nfailed == 1


def test_adding_ensure_is_not_an_io_change():
    @rule(outputs={'o': 'out/{x}.txt'}, matrix={'x': [1]})
    def r1(outputs, x):
        pass

    @rule(outputs={'o': Ensure('out/{x}.txt', min_size=3, checksum=True)}, matrix={'x': [1]})
    def r2(outputs, x):
        pass

    assert io_hash(r1) == io_hash(r2)


def test_check_outputs_does_not_adopt_a_failing_output(tmp_path):
    out = tmp_path / 'o.txt'
    out.write_text('')

    @rule(outputs={'o': Ensure(str(out))})
    def r(outputs):
        Path(outputs['o']).write_text('ok')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[r], metadata=db, check_outputs='fallback')
    assert len(rmk.plan()[0]) == 1  # empty file: not adopted


def test_checksum_recorded_when_opted_in(tmp_path):
    @rule(outputs={'a': Ensure(str(tmp_path / 'a.txt'), checksum=True),
                   'b': str(tmp_path / 'b.txt'), 'c': str(tmp_path / 'c.txt')})
    def r(outputs):
        Path(outputs['a']).write_text('hello')
        Path(outputs['b']).write_text('xx')
        Path(outputs['c']).write_text('yyy')

    rmk, nfailed, db = _run(tmp_path, [r])
    assert nfailed == 0
    (task,) = rmk.tasks()
    stats = db.get_output_stats(task)
    assert stats['a']['checksum'] == hashlib.sha256(b'hello').hexdigest()
    # Plain outputs without a checksum are summed, not stored one by one.
    assert stats['*'] == {'n_files': 2, 'total_bytes': 5, 'checksum': None}


def test_checksum_outputs_config_checksums_everything(tmp_path):
    @rule(outputs={'b': str(tmp_path / 'b.txt')}, config={'checksum_outputs': True})
    def r(outputs):
        Path(outputs['b']).write_text('xx')

    rmk, _, db = _run(tmp_path, [r])
    (task,) = rmk.tasks()
    assert db.get_output_stats(task)['b']['checksum'] == hashlib.sha256(b'xx').hexdigest()


def test_checksum_is_not_a_rerun_trigger(tmp_path):
    out = tmp_path / 'b.txt'

    @rule(outputs={'b': Ensure(str(out), checksum=True)})
    def r(outputs):
        Path(outputs['b']).write_text('xx')

    rmk, _, _ = _run(tmp_path, [r])
    out.write_text('changed by hand')
    assert rmk.plan()[0] == []
