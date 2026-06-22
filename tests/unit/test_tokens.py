import os
from pathlib import Path

import pytest

from remake.core.tokens import FileToken, S3Object, ZarrStore, as_token


def test_file_token_is_path_like():
    token = FileToken('data/out.txt')
    assert os.fspath(token) == 'data/out.txt'
    assert Path(token) == Path('data/out.txt')
    assert str(token) == 'data/out.txt'


def test_file_token_is_complete(tmp_path):
    token = FileToken(str(tmp_path / 'out.txt'))
    assert not token.is_complete()
    Path(token).write_text('x')
    assert token.is_complete()


def test_zarr_store_requires_consolidated_metadata(tmp_path):
    store = tmp_path / 'store.zarr'
    token = ZarrStore(str(store))
    store.mkdir()  # a half-written store also has a directory
    assert not token.is_complete()
    (store / '.zmetadata').write_text('{}')
    assert token.is_complete()


def test_format_interpolates_and_preserves_type():
    token = ZarrStore('data/{model}/{year}.zarr').format(model='era5', year=1980)
    assert isinstance(token, ZarrStore)
    assert token.path == 'data/era5/1980.zarr'


def test_s3_object_identity_and_format():
    token = S3Object('bucket-{model}', 'key/{year}').format(model='era5', year=1980)
    assert token.identity() == 's3://bucket-era5/key/1980'


def test_as_token_wraps_strings_passes_tokens():
    assert isinstance(as_token('a.txt'), FileToken)
    zarr = ZarrStore('a.zarr')
    assert as_token(zarr) is zarr
    with pytest.raises(TypeError):
        as_token(42)
