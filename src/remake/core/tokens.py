"""Output tokens.

Task completion is tracked in the metadata DB; tokens are the opt-in
verification layer — a way to ask the world, rather than the DB, whether an
output exists in finished form. Each token type encodes its own definition
of *finished*.

Path-backed tokens are transparent (`os.PathLike`): rule code passes them
straight to open(), Path(), xarray, zarr — no unwrapping.
"""
import abc
from pathlib import Path


class OutputToken(abc.ABC):
    @abc.abstractmethod
    def identity(self) -> str:
        """Stable string identifying this output (display, hashing)."""

    @abc.abstractmethod
    def is_complete(self) -> bool:
        """Has this output been successfully produced?"""

    @abc.abstractmethod
    def format(self, **kwargs) -> 'OutputToken':
        """A new token with matrix kwargs interpolated into the spec."""

    def __str__(self):
        return self.identity()

    def __repr__(self):
        return f'{type(self).__name__}({self.identity()!r})'


class PathToken(OutputToken):
    """Base for path-backed tokens."""

    def __init__(self, path):
        self.path = str(path)

    def __fspath__(self):
        return self.path

    def identity(self):
        return self.path

    def format(self, **kwargs):
        return type(self)(self.path.format(**kwargs))


class FileToken(PathToken):
    def is_complete(self):
        return Path(self.path).exists()


class ZarrStore(PathToken):
    def is_complete(self):
        # A half-written store also has a directory; only the consolidated
        # metadata written by zarr.consolidate_metadata() marks completion.
        return Path(self.path, '.zmetadata').exists()


class S3Object(OutputToken):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def identity(self):
        return f's3://{self.bucket}/{self.key}'

    def format(self, **kwargs):
        return type(self)(self.bucket.format(**kwargs), self.key.format(**kwargs))

    def is_complete(self):
        import boto3

        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except s3.exceptions.ClientError:
            return False


def as_token(value):
    """Wrap plain strings/Paths in FileToken; pass tokens through."""
    if isinstance(value, OutputToken):
        return value
    if isinstance(value, (str, Path)):
        return FileToken(value)
    raise TypeError(f'Cannot make an output token from {value!r}')
