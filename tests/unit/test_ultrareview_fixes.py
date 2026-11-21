"""Regression tests for the 0.9.0 ultrareview findings (2026-11-21)."""
import os
from pathlib import Path

import pytest

from remake import Dir, Ensure, Remake, Sqlite3Backend, rule
from remake.core.exceptions import SignatureError
from remake.core.tokens import OutputToken, S3Object, ZarrStore
from remake.loader import load_remake
from remake.metadata import TASK_STATUS_SUCCESS


def test_in_place_config_edit_in_remakefile_survives_load(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'rf.py').write_text('''
from remake import Remake, rule

@rule(outputs={'o': 'o.txt'})
def r(outputs):
    pass

rmk = Remake()
rmk.config['checksum_outputs'] = True
rmk.config.setdefault('slurm', {})['account'] = 'myacct'
rmk.rules_from_current_module()
''')
    rmk = load_remake('rf.py', finalize=False)
    assert rmk.config['checksum_outputs'] is True
    assert rmk.config['slurm']['account'] == 'myacct'


def test_same_file_spelled_absolute_and_relative_is_produced(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'data/out.txt'})
    def make(outputs):
        pass

    @rule(inputs={'i': str(tmp_path / 'data' / 'out.txt'), 'raw': './raw.txt'},
          outputs={'o': 'final.txt'}, depends_on=[make])
    def use(inputs, outputs):
        pass

    rmk = Remake(rules=[make, use])
    rmk.finalize()
    (task,) = [t for t in rmk.tasks() if t.rule is use]
    # Only the genuinely external input is reported.
    assert rmk.missing_external_inputs([task]) == {'use': ['./raw.txt']}


def test_directory_plain_output_with_checksums_succeeds(tmp_path):
    out = tmp_path / 'store.zarr'

    @rule(outputs={'o': str(out)})
    def write_store(outputs):
        os.makedirs(outputs['o'], exist_ok=True)
        Path(outputs['o'], 'chunk').write_text('x')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[write_store], metadata=db, config={'checksum_outputs': True})
    assert rmk.run() == 0
    (task,) = rmk.tasks()
    assert db.get_tasks_status([task])[task.key].status == TASK_STATUS_SUCCESS


def test_dataframe_with_unhashable_cells_renders_stably():
    pd = pytest.importorskip('pandas')
    from remake.core.scope import uses_hash

    df = pd.DataFrame({'vars': [['tas', 'pr'], ['ua']], 'n': [1, 2]})
    assert uses_hash({'df': df}) == uses_hash({'df': df.copy()})
    df2 = df.copy()
    df2.at[1, 'vars'] = ['va']
    assert uses_hash({'df': df}) != uses_hash({'df': df2})


class _Flaky(OutputToken):
    def __init__(self, name):
        self.name = name

    def identity(self):
        return f'flaky://{self.name}'

    def is_complete(self):
        raise RuntimeError('no credentials')

    def format(self, **kwargs):
        return self


def test_remote_or_failing_token_inputs_do_not_abort_the_check(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def boom(self):
        raise AssertionError('must not contact S3 at plan time')

    monkeypatch.setattr(S3Object, 'is_complete', boom)

    @rule(inputs={'s3': S3Object('b', 'k'), 'f': _Flaky('x')}, outputs={'o': 'o.txt'})
    def r(inputs, outputs):
        pass

    rmk = Remake(rules=[r])
    rmk.finalize()
    assert rmk.missing_external_inputs(list(rmk.tasks())) == {}


def test_datetime64_matrix_values_are_still_rejected(tmp_path, monkeypatch):
    np = pytest.importorskip('numpy')
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'o_{time}.txt'},
          matrix={'time': list(np.array(['2000-01-01'], dtype='datetime64[ns]'))})
    def r(outputs, time):
        pass

    rmk = Remake(rules=[r])
    with pytest.raises(SignatureError):
        rmk.finalize()
        list(rmk.tasks())


def test_numpy_numbers_are_still_canonicalised():
    np = pytest.importorskip('numpy')
    from remake.core.dag import _canonical_scalar

    assert type(_canonical_scalar(np.int64(3))) is int
    assert type(_canonical_scalar(np.str_('a'))) is str


def test_ensure_rejects_tokens_it_cannot_check():
    with pytest.raises(TypeError, match='ZarrStore'):
        Ensure(ZarrStore('out.zarr'), min_size=10)
    with pytest.raises(TypeError, match='S3Object'):
        Ensure(S3Object('b', 'k'))
    Ensure(Dir('d'))  # supported


def test_set_state_pending_upstream_does_not_rerun_downstream(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    @rule(outputs={'o': 'u.txt'})
    def U(outputs):
        Path(outputs['o']).write_text('u')

    @rule(inputs={'i': 'u.txt'}, outputs={'o': 'd.txt'}, depends_on=[U])
    def D(inputs, outputs):
        Path(outputs['o']).write_text('d')

    db = Sqlite3Backend(tmp_path / 'remake.db')
    rmk = Remake(rules=[U, D], metadata=db)
    assert rmk.run() == 0
    rmk.set_state("rule == 'U'", pending=True)
    runnable, _ = rmk.plan(query="rule == 'D'")
    assert runnable == []


def test_dir_owner_check_survives_an_unresolvable_task(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def outputs(x):
        if x == 2:
            raise FileNotFoundError('upstream product not there yet')
        return {'d': Dir(f'd{x}')}

    @rule(outputs=outputs, matrix={'x': [1, 2]})
    def r(outputs, x):
        os.makedirs(outputs['d'], exist_ok=True)

    rmk = Remake(rules=[r])
    rmk.finalize()
    rmk._check_duplicate_dirs()  # must not raise


def test_run_record_config_with_awkward_keys_is_serialised():
    from remake.metadata.sqlite3_backend import _config_json

    assert '8G' in _config_json({'mem_by_year': {2020: '8G', 'default': '4G'}})
    assert _config_json({'k': {(1, 2): 'x'}}) is not None
