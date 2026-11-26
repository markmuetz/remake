"""0.9.0 item 5: the Dir output token (design_docs/designs/dir_outputs.md)."""
import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
from loguru import logger

from remake import Dir, Ensure, Remake, RemakeError, Sqlite3Backend, rule
from remake.core.tokens import MANIFEST_NAME
from remake.metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS


def _run(tmp_path, rules, **config):
    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=rules, metadata=db, config=config)
    return rmk, rmk.run(), db


def _age(path, seconds=3600):
    t = os.stat(path).st_mtime - seconds
    os.utime(path, (t, t))


@pytest.fixture
def warnings_seen():
    records = []
    sink = logger.add(lambda m: records.append(m.record['message']), level='WARNING')
    yield records
    logger.remove(sink)


def _manifest(d):
    return json.loads(Path(d, MANIFEST_NAME).read_text())


def test_dir_is_pathlike_and_formats_per_task():
    tok = Dir('out/{year}')
    a, b = tok.format(year=1), tok.format(year=2)
    assert os.fspath(a) == 'out/1' and os.fspath(b) == 'out/2'
    a._start = 1.0  # per-instance state must not leak between formatted tokens
    assert tok.format(year=1)._start is None
    assert a is not b and b._start is None


def test_run_writes_manifest_and_stats(tmp_path):
    d = tmp_path / 'plots'

    @rule(outputs={'p': Dir(str(d))})
    def make(outputs):
        assert Path(outputs['p']).is_dir()  # remake made it
        (Path(outputs['p']) / 'a.txt').write_text('aaa')
        (Path(outputs['p']) / 'sub').mkdir()
        (Path(outputs['p']) / 'sub' / 'b.txt').write_text('bb')

    rmk, nfailed, db = _run(tmp_path, [make])
    assert nfailed == 0
    man = _manifest(d)
    assert set(man['files']) == {'a.txt', 'sub/b.txt'}
    assert man['n_files'] == 2 and man['total_bytes'] == 5 and man['n_stale'] == 0
    (task,) = rmk.tasks()
    assert task.outputs['p'].is_complete()
    # An output_stat row of its own, named after the output.
    assert db.get_output_stats(task) == {
        'p': {'n_files': 2, 'total_bytes': 5, 'checksum': None}}
    assert db.get_rule_output_bytes(['make']) == {'make': 5}
    # Nothing to do on a second run.
    assert rmk.plan() == ([], [])


def test_crash_partial_is_not_complete(tmp_path):
    d = tmp_path / 'plots'

    @rule(outputs={'p': Dir(str(d))})
    def crash(outputs):
        (Path(outputs['p']) / 'half.txt').write_text('x')
        raise RuntimeError('killed')

    rmk, nfailed, db = _run(tmp_path, [crash])
    assert nfailed == 1
    (task,) = rmk.tasks()
    assert (d / 'half.txt').exists()
    assert not (d / MANIFEST_NAME).exists()
    assert not task.outputs['p'].is_complete()
    assert db.get_tasks_status([task])[task.key].status == TASK_STATUS_FAILED


def test_rerun_deletes_only_the_manifest(tmp_path):
    d = tmp_path / 'plots'
    seen = {}

    @rule(outputs={'p': Dir(str(d))}, matrix={'n': [1]})
    def make(outputs, n):
        seen['manifest_during_run'] = (Path(outputs['p']) / MANIFEST_NAME).exists()
        seen['user_file_during_run'] = (Path(outputs['p']) / 'keep.txt').exists()
        if seen.get('fail'):
            raise RuntimeError('boom')
        (Path(outputs['p']) / 'keep.txt').write_text('v2')

    d.mkdir()
    (d / 'keep.txt').write_text('v1')
    (d / 'precious.txt').write_text('mine')
    rmk, nfailed, db = _run(tmp_path, [make])
    assert nfailed == 0 and (d / MANIFEST_NAME).exists()

    (task,) = rmk.tasks()
    seen['fail'] = True
    rmk.run(force=True)  # the rerun fails: the previous good files survive
    assert seen['manifest_during_run'] is False and seen['user_file_during_run']
    assert (d / 'keep.txt').read_text() == 'v2'
    assert (d / 'precious.txt').read_text() == 'mine'
    assert not (d / MANIFEST_NAME).exists()
    assert not task.outputs['p'].is_complete()


def test_stale_attribution(tmp_path, warnings_seen):
    d = tmp_path / 'plots'
    d.mkdir()
    (d / 'old.txt').write_text('left over')
    _age(d / 'old.txt')
    src = tmp_path / 'src.txt'
    src.write_text('copied')
    _age(src)

    @rule(outputs={'p': Dir(str(d))})
    def make(outputs):
        (Path(outputs['p']) / 'new.txt').write_text('fresh')
        # copy2 preserves the (old) mtime, but the *name* is new: produced.
        shutil.copy2(src, Path(outputs['p']) / 'copied.txt')

    rmk, nfailed, db = _run(tmp_path, [make])
    assert nfailed == 0
    man = _manifest(d)
    assert {k for k, v in man['files'].items() if v['stale']} == {'old.txt'}
    assert man['n_files'] == 2 and man['n_stale'] == 1
    assert man['stale_bytes'] == len('left over')
    assert any('stale' in m for m in warnings_seen)
    (task,) = rmk.tasks()
    assert task.outputs['p'].summary()['n_stale'] == 1
    assert db.get_output_stats(task)['p']['n_files'] == 2  # produced only


def test_copy2_over_existing_name_stays_stale(tmp_path):
    d = tmp_path / 'plots'
    d.mkdir()
    (d / 'x.txt').write_text('old')
    _age(d / 'x.txt')
    src = tmp_path / 'src.txt'
    src.write_text('new')
    _age(src)

    @rule(outputs={'p': Dir(str(d))})
    def make(outputs):
        shutil.copy2(src, Path(outputs['p']) / 'x.txt')

    _run(tmp_path, [make])
    assert _manifest(d)['files']['x.txt']['stale'] is True  # documented limitation


def test_ensure_nonempty_and_min_size(tmp_path):
    @rule(outputs={'p': Ensure(Dir(str(tmp_path / 'empty')))})
    def nothing(outputs):
        pass

    @rule(outputs={'p': Ensure(Dir(str(tmp_path / 'small')), min_size=10)})
    def small(outputs):
        (Path(outputs['p']) / 'f').write_text('abc')
        (Path(outputs['p']) / 'g').write_text('de')

    @rule(outputs={'p': Ensure(Dir(str(tmp_path / 'ok')), min_size=5)})
    def ok(outputs):
        (Path(outputs['p']) / 'f').write_text('abc')
        (Path(outputs['p']) / 'g').write_text('de')

    rmk, nfailed, db = _run(tmp_path, [nothing, small, ok])
    assert nfailed == 2
    status = {t.rule.name: db.get_tasks_status([t])[t.key] for t in rmk.tasks()}
    assert 'has no files' in status['nothing'].exception
    assert 'smaller than min_size=10' in status['small'].exception
    assert status['ok'].status == TASK_STATUS_SUCCESS
    # is_complete re-applies the cheap checks from the manifest aggregates.
    plain = Dir(str(tmp_path / 'ok'))
    assert plain.is_complete()
    assert Ensure(Dir(str(tmp_path / 'ok')), min_size=6).is_complete() is False
    assert Ensure(Dir(str(tmp_path / 'ok')), min_size=5).is_complete()


def test_checksum_per_file(tmp_path):
    d = tmp_path / 'plots'

    @rule(outputs={'p': Ensure(Dir(str(d)), checksum=True)})
    def make(outputs):
        (Path(outputs['p']) / 'a').write_bytes(b'hello')

    _run(tmp_path, [make])
    assert _manifest(d)['files']['a']['sha256'] == hashlib.sha256(b'hello').hexdigest()

    d2 = tmp_path / 'plain'

    @rule(outputs={'p': Dir(str(d2))})
    def other(outputs):
        (Path(outputs['p']) / 'a').write_bytes(b'hello')

    _run(tmp_path, [other], checksum_outputs=True)  # the global opt-in
    assert 'sha256' in _manifest(d2)['files']['a']
    d3 = tmp_path / 'none'

    @rule(outputs={'p': Dir(str(d3))})
    def third(outputs):
        (Path(outputs['p']) / 'a').write_bytes(b'hello')

    _run(tmp_path, [third])
    assert 'sha256' not in _manifest(d3)['files']['a']


def test_manifest_write_is_atomic_no_tmp_left(tmp_path):
    d = tmp_path / 'plots'

    @rule(outputs={'p': Dir(str(d))})
    def make(outputs):
        (Path(outputs['p']) / 'a').write_text('x')

    _run(tmp_path, [make])
    assert sorted(os.listdir(d)) == ['.remake-manifest.json', 'a']


def test_duplicate_dir_is_an_error_naming_both_tasks(tmp_path):
    shared = str(tmp_path / 'shared')

    @rule(outputs={'p': Dir(shared)})
    def one(outputs):
        pass

    @rule(outputs={'p': Dir(shared + '/')})  # same place, different spelling
    def two(outputs):
        pass

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[one, two], metadata=db)
    with pytest.raises(RemakeError) as exc:
        rmk.run()
    assert 'one[' in str(exc.value) and 'two[' in str(exc.value)
    assert not Path(shared).exists()  # nothing ran

    @rule(outputs={'p': Dir(str(tmp_path / 'by_{n}'))}, matrix={'n': [1, 2]})
    def distinct(outputs, n):
        pass

    @rule(outputs={'p': Dir(str(tmp_path / 'same'))}, matrix={'n': [1, 2]})
    def clash(outputs, n):
        pass

    rmk = Remake(rules=[distinct, clash], metadata=Sqlite3Backend(':memory:'))
    with pytest.raises(RemakeError, match='clash'):
        rmk.run()


def test_set_state_adopts_existing_dir_without_manifest(tmp_path):
    d = tmp_path / 'plots'
    d.mkdir()
    (d / 'a.txt').write_text('aa')
    (d / 'b.txt').write_text('bbb')
    calls = []

    @rule(outputs={'p': Dir(str(d))})
    def make(outputs):
        calls.append(1)

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[make], metadata=db)
    dry = rmk.set_state(None, success=True, check_outputs=True, dry_run=True)
    assert len(dry['tasks']) == 1 and not (d / MANIFEST_NAME).exists()
    res = rmk.set_state(None, success=True, check_outputs=True)
    assert len(res['tasks']) == 1 and res['skipped'] == 0
    man = _manifest(d)
    assert man['n_files'] == 2 and man['n_stale'] == 0 and man['total_bytes'] == 5
    assert rmk.run() == 0 and not calls  # adopted: nothing reruns


def test_set_state_does_not_adopt_a_missing_dir(tmp_path):
    @rule(outputs={'p': Dir(str(tmp_path / 'nope'))})
    def make(outputs):
        pass

    rmk = Remake(rules=[make], metadata=Sqlite3Backend(':memory:'))
    res = rmk.set_state(None, success=True, check_outputs=True)
    assert res['skipped'] == 1 and not res['tasks']


def test_migration_recipe_files_to_dir_causes_no_rerun(tmp_path):
    out = tmp_path / 'out'
    calls = []

    @rule(outputs={'a': str(out / 'a.txt'), 'b': str(out / 'b.txt')}, name='gen')
    def gen_files(outputs):
        calls.append('files')
        for p in outputs.values():
            Path(p).write_text('x')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    Remake(rules=[gen_files], metadata=db).run()
    assert calls == ['files']
    db.close()

    # Convert the rule's N file outputs to one Dir: an io change, so it reruns...
    @rule(outputs={'d': Dir(str(out))}, name='gen')
    def gen_dir(outputs):
        calls.append('dir')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[gen_dir], metadata=db)
    runnable, _ = rmk.plan()
    assert len(runnable) == 1
    # ... unless set-state --success --check-outputs restamps the io code id.
    res = rmk.set_state(None, success=True, check_outputs=True)
    assert len(res['tasks']) == 1
    assert rmk.plan() == ([], [])
    assert rmk.run() == 0 and calls == ['files']
    man = _manifest(out)
    assert set(man['files']) == {'a.txt', 'b.txt'} and man['n_stale'] == 0


def test_upgrade_from_db_without_output_stat_reruns_nothing(tmp_path):
    d = tmp_path / 'plots'
    calls = []

    @rule(outputs={'p': Dir(str(d)), 'q': str(tmp_path / 'q.txt')})
    def make(outputs):
        calls.append(1)
        (Path(outputs['p']) / 'a').write_text('x')
        Path(outputs['q']).write_text('q')

    path = tmp_path / 'remake.db'
    db = Sqlite3Backend(path)
    Remake(rules=[make], metadata=db).run()
    db.conn.execute('DROP TABLE output_stat')  # a DB from before this change
    db.conn.commit()
    db.close()

    db = Sqlite3Backend(path)  # opens, re-adds the table
    rmk = Remake(rules=[make], metadata=db)
    assert rmk.plan() == ([], [])
    assert rmk.run() == 0 and calls == [1]
    (task,) = rmk.tasks()
    assert db.get_output_stats(task) == {}  # old records have no stats, harmlessly
    assert rmk.info()['totals']['output_bytes'] == 0


def test_task_info_shows_stats_and_stale(tmp_path):
    d = tmp_path / 'plots'
    d.mkdir()
    (d / 'old').write_text('12345')
    _age(d / 'old')

    @rule(outputs={'p': Dir(str(d)), 'f': str(tmp_path / 'f.txt')})
    def make(outputs):
        (Path(outputs['p']) / 'a').write_text('xy')
        Path(outputs['f']).write_text('abc')

    rmk, _, db = _run(tmp_path, [make])
    (task,) = rmk.tasks()
    info = rmk.task_info(task)
    assert info['outputs']['p']['dir']['n_stale'] == 1
    assert info['output_stats']['p']['n_files'] == 1
    assert info['output_stats']['*'] == {'n_files': 1, 'total_bytes': 3, 'checksum': None}
    summary = rmk.info()
    assert summary['rules'][0]['output_bytes'] == 5
    assert summary['totals']['output_bytes'] == 5


def test_lint_prefix_matching_and_nesting(tmp_path):
    @rule(outputs={'d': Dir(str(tmp_path / 'tiles'))})
    def make_tiles(outputs):
        pass

    @rule(inputs={'t': str(tmp_path / 'tiles/a/b.nc')}, outputs={'o': str(tmp_path / 'o1')},
          depends_on=[make_tiles])
    def use_ok(inputs, outputs):
        pass

    @rule(inputs={'t': str(tmp_path / 'tiles/a/b.nc')}, outputs={'o': str(tmp_path / 'o2')})
    def use_no_dep(inputs, outputs):
        pass

    @rule(inputs={'t': str(tmp_path / 'tilesX/a.nc')}, outputs={'o': str(tmp_path / 'o3')})
    def use_other(inputs, outputs):
        pass

    rmk = Remake(rules=[make_tiles, use_ok, use_no_dep, use_other],
                 metadata=Sqlite3Backend(':memory:'))
    rows = {(r['kind'], r['rule']): r for r in rmk.lint()}
    assert ('missing_dependency', 'use_no_dep') in rows
    assert rows[('missing_dependency', 'use_no_dep')]['other_rule'] == 'make_tiles'
    assert not [k for k in rows if k[1] == 'use_ok']  # wired through the prefix
    assert ('external', 'use_other') in rows  # a sibling name is not inside the Dir

    @rule(outputs={'d': Dir(str(tmp_path / 'tiles/inner'))})
    def nested(outputs):
        pass

    @rule(outputs={'f': str(tmp_path / 'tiles/x.txt')})
    def stray(outputs):
        pass

    rmk = Remake(rules=[make_tiles, nested, stray], metadata=Sqlite3Backend(':memory:'))
    rows = {(r['kind'], r['rule']): r for r in rmk.lint()}
    assert rows[('dir_nested', 'nested')]['other_rule'] == 'make_tiles'
    assert rows[('file_in_dir', 'stray')]['other_rule'] == 'make_tiles'


def test_claim_is_a_deprecated_alias_of_adopt(tmp_path):
    """0.11 item 7: Dir.claim() (the 0.9 name) warns, and goes at 1.0."""
    from remake import Dir

    d = tmp_path / 'd'
    d.mkdir()
    (d / 'f.txt').write_text('x')
    with pytest.warns(DeprecationWarning, match='adopt'):
        Dir(str(d)).claim()
    assert Dir(str(d)).is_complete()
