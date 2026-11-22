"""Regressions from the Fable review of release/0.10.0 (2026-11-22)."""
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from remake import Dir, Remake, Sqlite3Backend, load_remake, rule
from remake.core.tokens import FileToken, complete_many


def test_files_inside_a_symlinked_dir_are_produced_by_it(tmp_path, monkeypatch):
    # Finding 1: out/dd -> scratch/dd, the usual HPC layout.
    (tmp_path / 'scratch/dd').mkdir(parents=True)
    (tmp_path / 'out').mkdir()
    os.symlink(tmp_path / 'scratch/dd', tmp_path / 'out/dd')
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'d': Dir('out/dd')})
    def make(outputs):
        Path(outputs['d'], 'x.txt').write_text('x')

    @rule(inputs={'x': 'out/dd/x.txt'}, outputs={'o': 'out/y.txt'}, depends_on=[make])
    def use(inputs, outputs):
        Path(outputs['o']).write_text(Path(inputs['x']).read_text())

    rmk = Remake(rules=[make, use], metadata=Sqlite3Backend(':memory:'))
    rmk.finalize()
    assert rmk.missing_external_inputs(rmk.tasks()) == {}
    assert rmk.lint() == []
    assert rmk.run() == 0 and (tmp_path / 'out/y.txt').read_text() == 'x'


def test_complete_many_agrees_with_is_complete_on_odd_spellings(tmp_path):
    # Finding 2: case-insensitive names, trailing slashes, unreadable dirs.
    d = tmp_path / 'data'
    d.mkdir()
    for i in range(10):
        (d / f'f{i}.NC').write_text('x')
    tokens = [FileToken(str(d / f'f{i}.nc')) for i in range(10)]
    tokens += [FileToken(str(d) + '/')] * 8
    assert complete_many(tokens) == [t.is_complete() for t in tokens]

    noread = tmp_path / 'noread'
    noread.mkdir()
    for i in range(9):
        (noread / f'g{i}').write_text('x')
    noread.chmod(0o111)  # search-only: listing fails, stat works
    try:
        tokens = [FileToken(str(noread / f'g{i}')) for i in range(9)]
        assert complete_many(tokens) == [True] * 9
    finally:
        noread.chmod(0o755)


def test_module_scan_claims_rules_built_from_functions_defined_elsewhere(tmp_path, monkeypatch):
    # Finding 3: a factory or rule(...)(lib.fn) in the remakefile is its rule.
    (tmp_path / 'lib_r3.py').write_text(textwrap.dedent('''
        from pathlib import Path
        from remake import rule

        def make_copy_rule(name, dst):
            def fn(outputs):
                Path(outputs['o']).write_text('x')
            fn.__name__ = fn.__qualname__ = name
            return rule(outputs={'o': dst})(fn)

        def shared(outputs):
            Path(outputs['o']).write_text('x')

        @rule(outputs={'o': 'imported.txt'})
        def imported(outputs):
            pass
    '''))
    (tmp_path / 'pipe_r3.py').write_text(textwrap.dedent('''
        from remake import Remake, rule
        import lib_r3
        from lib_r3 import imported

        copy_a = lib_r3.make_copy_rule('copy_a', 'a.txt')
        shared_b = rule(name='shared_b', outputs={'o': 'b.txt'})(lib_r3.shared)

        rmk = Remake()
        rmk.rules_from_current_module()
    '''))
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        rmk = load_remake('pipe_r3.py', finalize=False)
        assert sorted(r.name for r in rmk.rules) == ['copy_a', 'shared_b']
    finally:
        for m in ('lib_r3', 'pipe_r3'):
            sys.modules.pop(m, None)


@pytest.mark.real_anchoring
def test_generator_methods_are_anchored_per_step(tmp_path):
    # Finding 4: explain_tasks/why ran in the caller's cwd.
    proj, elsewhere = tmp_path / 'proj', tmp_path / 'else'
    proj.mkdir()
    elsewhere.mkdir()
    (proj / 'pipeline.py').write_text(textwrap.dedent('''
        import os
        from pathlib import Path
        from remake import Remake, rule

        @rule(outputs={'o': 'out.txt'})
        def one(outputs):
            Path(outputs['o']).write_text('x')

        rmk = Remake(rules=[one], check_outputs='always')
        assert rmk.run() == 0
        here = os.getcwd()
        for task, will_run, reasons in rmk.explain_tasks(list(rmk.tasks())):
            assert os.getcwd() == here  # restored between steps
            assert not will_run, reasons
        print('ok')
    '''))
    out = subprocess.run([sys.executable, str(proj / 'pipeline.py')], cwd=elsewhere,
                         capture_output=True, text=True)
    assert out.stdout.strip() == 'ok', out.stderr


def test_load_remake_with_a_subdirectory_path_runs_under_multiproc(tmp_path, monkeypatch):
    # Finding 5: workers re-loaded 'sub/pipe.py' from inside sub/.
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub/pipe_r5.py').write_text(textwrap.dedent('''
        from pathlib import Path
        from remake import Remake, rule

        @rule(outputs={'o': 'out_{i}.txt'}, matrix={'i': [1, 2]})
        def make_r5(outputs, i):
            Path(outputs['o']).write_text(str(i))

        rmk = Remake()
        rmk.rules_from_current_module()
    '''))
    monkeypatch.chdir(tmp_path)
    from remake.executors import MultiprocExecutor

    rmk = load_remake('sub/pipe_r5.py')
    try:
        assert Path(rmk.remakefile).is_absolute()
        rmk.run(executor=MultiprocExecutor(rmk, nproc=2))
        assert (tmp_path / 'sub/out_2.txt').read_text() == '2'
    finally:
        sys.modules.pop('pipe_r5', None)


def test_programmatic_metadata_dir_holds_the_database(tmp_path, monkeypatch):
    # Finding 6: finalize() made the default backend before the config.
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'o.txt'})
    def make(outputs):
        Path(outputs['o']).write_text('x')

    rmk = Remake(rules=[make], config={'metadata_dir': 'meta'})
    assert rmk.run() == 0
    assert (tmp_path / 'meta/remake.db').exists()
    assert not (tmp_path / '.remake').exists()


def test_symlinked_remakefile_stays_the_link(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #6: workers must reload the link, not its target.
    (tmp_path / 'shared').mkdir()
    (tmp_path / 'proj').mkdir()
    (tmp_path / 'shared/pipe_r6.py').write_text(textwrap.dedent('''
        from pathlib import Path
        from remake import Remake, rule

        @rule(outputs={'o': 'out_{i}.txt'}, matrix={'i': [1, 2]})
        def make_r6(outputs, i):
            Path(outputs['o']).write_text(str(i))

        rmk = Remake()
        rmk.rules_from_current_module()
    '''))
    os.symlink(tmp_path / 'shared/pipe_r6.py', tmp_path / 'proj/pipe_r6.py')
    monkeypatch.chdir(tmp_path)
    from remake.executors import MultiprocExecutor

    rmk = load_remake('proj/pipe_r6.py')
    try:
        assert rmk.remakefile == str(tmp_path / 'proj/pipe_r6.py')
        rmk.run(executor=MultiprocExecutor(rmk, nproc=2))
        assert (tmp_path / 'proj/out_2.txt').read_text() == '2'
        assert rmk.plan() == ([], [])
    finally:
        sys.modules.pop('pipe_r6', None)


def test_environment_write_failure_keeps_the_task_result(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #7: an OSError writing the package file must not
    # lose the sidecar; the list stays inline and still reaches the DB.
    import json

    from remake.metadata import sidecar
    from remake.metadata.sidecar import SidecarWriter, task_result_path

    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'o.txt'})
    def r7(outputs):
        pass

    rmk = Remake(rules=[r7], metadata=Sqlite3Backend(tmp_path / 'r.db'))
    rmk.finalize()
    (t,) = rmk.tasks()

    def boom(*a):
        raise OSError('quota')

    monkeypatch.setattr(sidecar, 'write_environment', boom)
    env = {'fingerprint': 'f' * 16, 'python_version': '3.12', 'packages': ['a==1']}
    SidecarWriter(run_seq=1).update_task(t, 1, resources={}, execution={'env': env})
    payload = json.loads(task_result_path('r7', t.key).read_text())
    assert payload['execution']['env']['packages'] == ['a==1']
    rmk.metadata.ingest_sidecars(rmk.rules)
    (row,) = rmk.metadata.get_history(t, limit=5)
    assert rmk.metadata.get_environment(row['env_id'])['packages'] == ['a==1']


@pytest.mark.real_anchoring
def test_default_backend_made_elsewhere_warns_and_stays_with_its_lock(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #9.
    from loguru import logger

    proj, elsewhere = tmp_path / 'proj', tmp_path / 'else'
    proj.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    backend = Sqlite3Backend()
    assert os.path.isabs(backend.dbloc)
    msgs = []
    sink = logger.add(lambda m: msgs.append(str(m)), level='WARNING')
    try:
        Remake(rules=[], metadata=backend, project_dir=proj).finalize()
    finally:
        logger.remove(sink)
    assert any('not in this pipeline' in m for m in msgs), msgs


def test_custom_pathlike_token_gets_its_parent_dir(tmp_path, monkeypatch):
    # /code-review 2026-11-22 #10: an os.PathLike token that isn't a PathToken.
    from remake.core.tokens import OutputToken

    class MyNc(OutputToken):
        def __init__(self, path):
            self.p = str(path)

        def __fspath__(self):
            return self.p

        def identity(self):
            return self.p

        def is_complete(self):
            return os.path.exists(self.p)

        def format(self, **kw):
            return MyNc(self.p.format(**kw))

    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': MyNc('out/{x}/a.nc')}, matrix={'x': [1]})
    def r10(outputs, x):
        Path(outputs['o']).write_text('x')

    rmk = Remake(rules=[r10], metadata=Sqlite3Backend(':memory:'))
    assert rmk.run() == 0 and (tmp_path / 'out/1/a.nc').exists()
