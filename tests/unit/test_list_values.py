"""List-valued inputs and outputs (0.10 decision D8; bug 02, review L2)."""
from pathlib import Path

import pytest

from remake import Remake, Sqlite3Backend, rule
from remake.core.exceptions import SignatureError
from remake.core.task import paths_digest
from remake.core.tokens import FileToken


def _pipeline(tmp_path):
    @rule(outputs={'files': ['a/{i}.txt', 'b/{i}.txt'], 'one': 'c/{i}.txt'},
          matrix={'i': [1, 2]})
    def make(outputs, i):
        assert isinstance(outputs['files'], list) and len(outputs['files']) == 2
        for p in [*outputs['files'], outputs['one']]:
            Path(p).write_text(str(i))

    @rule(inputs={'files': ['a/{i}.txt', 'b/{i}.txt']}, outputs={'o': 'd/{i}.txt'},
          matrix={'i': [1, 2]}, depends_on=[make])
    def use(inputs, outputs, i):
        Path(outputs['o']).write_text(''.join(Path(p).read_text() for p in inputs['files']))

    return make, use


def test_list_values_are_formatted_per_element_and_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make, use = _pipeline(tmp_path)
    rmk = Remake(rules=[make, use], metadata=Sqlite3Backend(tmp_path / 'r.db'))
    assert rmk.run() == 0
    assert Path('d/1.txt').read_text() == '11'
    t = next(t for t in rmk.tasks() if t.rule is make and t.kwargs == {'i': 1})
    assert [str(p) for p in t.outputs['files']] == ['a/1.txt', 'b/1.txt']
    assert all(isinstance(p, FileToken) for p in t.outputs['files'])
    assert rmk.plan() == ([], [])


def test_lint_and_task_info_see_each_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make, use = _pipeline(tmp_path)
    rmk = Remake(rules=[make, use], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    assert rmk.lint() == []  # every list input is made by `make`
    t = next(t for t in rmk.tasks() if t.rule is use and t.kwargs == {'i': 2})
    info = rmk.task_info(t)
    assert set(info['inputs']) == {'files[0]', 'files[1]'}
    assert info['inputs']['files[1]']['path'] == 'b/2.txt'
    spec = rmk.export_task(t)
    assert spec['inputs']['files'] == ['a/2.txt', 'b/2.txt']


def test_rerun_of_one_upstream_reruns_only_its_reader(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make, use = _pipeline(tmp_path)
    rmk = Remake(rules=[make, use], metadata=Sqlite3Backend(tmp_path / 'r.db'))
    rmk.run()
    rmk.set_state("rule == 'make' and i == 1", pending=True)
    runnable, _ = rmk.plan()
    assert sorted((t.rule.name, t.kwargs['i']) for t in runnable) == [('make', 1), ('use', 1)]


def test_callable_returning_a_tuple_works_in_task_info(tmp_path, monkeypatch):
    # Bug 02: task-info crashed on Path(tuple).
    monkeypatch.chdir(tmp_path)

    @rule(inputs=lambda: {'pair': ('x.txt', 'y.txt')}, outputs={'o': 'z.txt'})
    def r(inputs, outputs):
        pass

    rmk = Remake(rules=[r], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    (t,) = rmk.tasks()
    info = rmk.task_info(t)
    assert [v['path'] for v in info['inputs'].values()] == ['x.txt', 'y.txt']


def test_nested_lists_are_rejected_at_definition():
    with pytest.raises(SignatureError, match='flat'):
        @rule(outputs={'o': [['a.txt']]})
        def r(outputs):
            pass


def test_digest_of_a_list_of_strings_is_unchanged_from_0_9(tmp_path, monkeypatch):
    # 0.9 stored str(list) for a callable returning a list: an upgrade must
    # not rerun it.
    monkeypatch.chdir(tmp_path)

    @rule(inputs=lambda: {'xs': ['a.txt', 'b.txt']}, outputs={'o': 'o.txt'})
    def r(inputs, outputs):
        pass

    rmk = Remake(rules=[r], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    (t,) = rmk.tasks()
    from hashlib import sha1
    items = sorted([('i', repr('xs'), str(['a.txt', 'b.txt'])), ('o', repr('o'), 'o.txt')])
    assert paths_digest(t) == f'1:1:{sha1(repr(items).encode()).hexdigest()}'


def test_renamed_methods_keep_deprecated_aliases(tmp_path, monkeypatch):
    # 0.10 item 5d: names match the CLI verbs; the old ones warn until 1.0.
    monkeypatch.chdir(tmp_path)
    make, use = _pipeline(tmp_path)
    rmk = Remake(rules=[make, use], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    t = next(iter(rmk.tasks()))
    with pytest.warns(DeprecationWarning, match='export_task'):
        assert rmk.task_spec(t) == rmk.export_task(t)
    with pytest.warns(DeprecationWarning, match='info'):
        assert rmk.status_summary()['totals'] == rmk.info()['totals']
    assert rmk.history(t) == [] and rmk.history(runs=True) == []


def test_digest_of_a_callable_list_of_paths_is_unchanged_from_0_9(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #2: 0.9 stored str(list), "[PosixPath('a.txt')]".
    from hashlib import sha1

    monkeypatch.chdir(tmp_path)

    @rule(inputs=lambda: {'xs': [Path('a.txt'), Path('b.txt')]}, outputs={'o': 'o.txt'})
    def r(inputs, outputs):
        pass

    rmk = Remake(rules=[r], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    (t,) = rmk.tasks()
    items = sorted([('i', repr('xs'), str([Path('a.txt'), Path('b.txt')])), ('o', repr('o'), 'o.txt')])
    assert paths_digest(t) == f'1:1:{sha1(repr(items).encode()).hexdigest()}'
