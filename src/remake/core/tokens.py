"""Output tokens.

Task completion is tracked in the metadata DB; tokens are the opt-in
verification layer — a way to ask the world, rather than the DB, whether an
output exists in finished form. Each token type encodes its own definition
of *finished*.

Path-backed tokens are transparent (`os.PathLike`): rule code passes them
straight to open(), Path(), xarray, zarr — no unwrapping.
"""
import abc
import copy
import hashlib
import os
from pathlib import Path


def sha256_file(path, bufsize=1 << 20):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


class OutputToken(abc.ABC):
    # Output validation options (0.9.0 item 4), set by `Ensure(...)`. Not part
    # of the token's identity or repr, so adding or changing validation is
    # not an inputs/outputs change (no rerun).
    nonempty = False
    min_size = None
    checksum = False

    @abc.abstractmethod
    def identity(self) -> str:
        """Stable string identifying this output (display, hashing)."""

    @abc.abstractmethod
    def is_complete(self) -> bool:
        """Has this output been successfully produced?"""

    @abc.abstractmethod
    def format(self, **kwargs) -> 'OutputToken':
        """A new token with matrix kwargs interpolated into the spec."""

    def validate(self):
        """Called in the task process after the rule function returns: None
        if this output is acceptable, else a message (the task then fails).
        Only the checks requested with `Ensure` apply; a plain output is not
        checked (as before 0.9.0)."""
        return None

    def before_run(self):
        """Called in the task process before the rule function runs."""

    def after_success(self, checksum=False):
        """Called after a successful run and validation: stats for this
        output — {'n_files', 'total_bytes', 'checksum'} (any may be None) —
        or None for nothing to record. `checksum`: the rule/global
        `checksum_outputs` opt-in; `Ensure(checksum=True)` also turns it on."""
        return None

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
        # A copy, so Ensure options (and any subclass state) carry over.
        new = copy.copy(self)
        new.path = self.path.format(**kwargs)
        return new


class FileToken(PathToken):
    def _problem(self):
        try:
            size = os.stat(self.path).st_size
        except FileNotFoundError:
            return 'was not written'
        if self.nonempty and size == 0:
            return 'is empty'
        if self.min_size is not None and size < self.min_size:
            return f'is {size} bytes, smaller than min_size={self.min_size}'
        return None

    def is_complete(self):
        # Includes the Ensure checks, so check_outputs and set-state
        # --check-outputs never adopt a file that would fail validation.
        if not (self.nonempty or self.min_size is not None):
            return Path(self.path).exists()
        return self._problem() is None

    def validate(self):
        if not (self.nonempty or self.min_size is not None or self.checksum):
            return None
        problem = self._problem()
        return f'{self.path} {problem}' if problem else None

    def after_success(self, checksum=False):
        try:
            size = os.stat(self.path).st_size
        except FileNotFoundError:
            return None
        digest = sha256_file(self.path) if (checksum or self.checksum) else None
        return {'n_files': 1, 'total_bytes': size, 'checksum': digest}


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


def Ensure(output, *, nonempty=True, min_size=None, checksum=False):
    """Declare checks on an output, run when its task completes; a failed
    check fails the task. Returns the same kind of token (a path becomes a
    FileToken), so it works wherever the output did:

        outputs={'nc': Ensure('out/{year}.nc', min_size=1024, checksum=True)}

    nonempty: the output is not empty (for a Dir, holds at least one file).
    min_size: at least this many bytes (a Dir: in total).
    checksum: record its sha256 (a Dir: per file, in its manifest) — opt-in,
      since it re-reads every byte; `checksum_outputs=True` in a rule's or the
      pipeline's config turns it on for all outputs. Recorded, never compared:
      a checksum is not a rerun trigger.

    The checks are not part of the output's identity: adding or changing them
    doesn't rerun the task, and `check_outputs`/`set-state --check-outputs`
    apply them before adopting an output."""
    token = copy.copy(as_token(output))
    token.nonempty = nonempty
    token.min_size = min_size
    token.checksum = checksum
    return token


def as_token(value):
    """Wrap plain strings/Paths in FileToken; pass tokens through."""
    if isinstance(value, OutputToken):
        return value
    if isinstance(value, (str, Path)):
        return FileToken(value)
    raise TypeError(f'Cannot make an output token from {value!r}')
