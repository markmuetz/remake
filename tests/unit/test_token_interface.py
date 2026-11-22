"""The frozen output-token interface (0.10 item 5a)."""
import os

from remake.core.tokens import (Dir, FileToken, OutputStat, OutputToken, S3Object,
                                ZarrStore, canonical, canonical_path, complete_many,
                                is_remote)


def test_complete_many_matches_is_complete_in_order(tmp_path):
    tokens = [FileToken(tmp_path / f'f{i}') for i in range(20)]
    for t in tokens[::3]:
        open(t.path, 'w').close()
    os.symlink(tmp_path / 'nowhere', tmp_path / 'f1')  # broken link: not complete
    (tmp_path / 'z.zarr').mkdir()
    mixed = tokens + [Dir(tmp_path / 'd'), ZarrStore(tmp_path / 'z.zarr')]
    assert complete_many(mixed) == [t.is_complete() for t in mixed]


def test_batch_respects_ensure_checks_and_overrides(tmp_path):
    from remake import Ensure

    (tmp_path / 'empty').write_text('')
    checked = Ensure(str(tmp_path / 'empty'))  # nonempty=True by default

    class Always(FileToken):
        def is_complete(self):
            return True

    assert complete_many([checked, Always(tmp_path / 'missing')]) == [False, True]


def test_stat(tmp_path):
    f = tmp_path / 'f'
    assert FileToken(f).stat() == OutputStat(exists=False)
    f.write_text('abc')
    st = FileToken(f).stat()
    assert st.exists and st.size == 3 and st.mtime
    assert Dir(tmp_path / 'nope').stat().exists is False


def test_canonical_spellings_agree(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert canonical('./a/../b.txt') == canonical(str(tmp_path / 'b.txt'))
    assert canonical(FileToken('b.txt')) == canonical_path('b.txt')
    assert canonical('s3://bucket/key') == 's3://bucket/key'


def test_remote_flag():
    assert is_remote(S3Object('b', 'k')) and is_remote('https://x/y')
    assert not is_remote('data/x.nc') and not is_remote(FileToken('x'))


def test_adopt_defaults_to_is_complete_and_dir_writes_its_manifest(tmp_path):
    d = tmp_path / 'd'
    d.mkdir()
    (d / 'x').write_text('x')
    token = Dir(d)
    assert token.adopt(write=False) and not token.is_complete()
    assert token.adopt() and token.is_complete()
    assert FileToken(d / 'x').adopt() is True
    assert OutputToken.adopt is not Dir.adopt
