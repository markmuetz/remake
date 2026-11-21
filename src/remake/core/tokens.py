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
import json
import os
import time
from pathlib import Path

from loguru import logger


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


MANIFEST_NAME = '.remake-manifest.json'
# A file written just after the task started can carry an mtime a few ms
# before our own clock reading (coarse kernel timestamps); this much slack
# keeps it classed as produced.
_MTIME_SLACK_S = 0.02


class Dir(PathToken):
    """A directory of variable content, produced by one task (design:
    design_docs/designs/dir_outputs.md).

        outputs={'plots': Dir('data/plots/{year}')}

    Every file in it is attributed to the task. Completion is a hidden
    manifest (`.remake-manifest.json`) that remake writes inside the dir
    after the rule returns successfully, not the dir's existence, so a task
    killed mid-write is not mistaken for finished. On a rerun remake deletes
    only the manifest, never your files; files that were not produced by the
    latest run are recorded as `stale` (and warned about). Don't wrap a
    ZarrStore in a Dir; it has its own completion marker."""

    # Per-task state, set in before_run on the per-task (formatted) token.
    _start = None
    _snapshot = None
    _scanned = None

    @property
    def manifest_path(self):
        return os.path.join(self.path, MANIFEST_NAME)

    def format(self, **kwargs):
        new = super().format(**kwargs)
        new._start = new._snapshot = new._scanned = None
        return new

    def _ensure_requested(self):
        return bool(self.nonempty or self.min_size is not None)

    def _walk(self):
        """{relative posix path: (size, mtime)} of every file under the dir,
        other than remake's own manifest files."""
        found = {}
        for root, dirs, files in os.walk(self.path):
            dirs.sort()
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, self.path)
                if rel in (MANIFEST_NAME, MANIFEST_NAME + '.tmp'):
                    continue
                try:
                    st = os.stat(full)
                except FileNotFoundError:
                    continue
                found[Path(rel).as_posix()] = (st.st_size, st.st_mtime)
        return found

    def _scan(self, claim_all=False):
        """Walk the dir and partition into produced / stale: produced = new
        name since before_run, or mtime at or after the task's start. With
        `claim_all` (adoption) everything is produced."""
        files = {}
        for rel, (size, mtime) in self._walk().items():
            produced = (claim_all or self._start is None
                        or rel not in (self._snapshot or ())
                        or mtime >= self._start - _MTIME_SLACK_S)
            files[rel] = {'size': size, 'mtime': mtime, 'stale': not produced}
        return files

    @staticmethod
    def _aggregates(files):
        produced = [f for f in files.values() if not f['stale']]
        stale = [f for f in files.values() if f['stale']]
        return {
            'n_files': len(produced),
            'total_bytes': sum(f['size'] for f in produced),
            'n_stale': len(stale),
            'stale_bytes': sum(f['size'] for f in stale),
        }

    def _check(self, n_files, total_bytes):
        if self.nonempty and n_files == 0:
            return 'has no files'
        if self.min_size is not None and total_bytes < self.min_size:
            return (f'holds {total_bytes} bytes in total, smaller than '
                    f'min_size={self.min_size}')
        return None

    def manifest(self):
        """The manifest as a dict, or None if absent or unreadable."""
        try:
            with open(self.manifest_path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def summary(self):
        """Manifest aggregates {'n_files', 'total_bytes', 'n_stale',
        'stale_bytes'}, or None without a (readable) manifest."""
        data = self.manifest()
        if data is None:
            return None
        return {k: data.get(k, 0) for k in
                ('n_files', 'total_bytes', 'n_stale', 'stale_bytes')}

    def is_complete(self):
        data = self.manifest()
        if data is None:
            return False
        if self._ensure_requested():
            return self._check(data.get('n_files', 0), data.get('total_bytes', 0)) is None
        return True

    def before_run(self):
        os.makedirs(self.path, exist_ok=True)
        try:
            os.remove(self.manifest_path)  # incomplete while the task runs
        except FileNotFoundError:
            pass
        self._snapshot = set(self._walk())
        self._scanned = None
        self._start = time.time()

    def validate(self):
        if not (self._ensure_requested() or self.checksum):
            return None
        if not os.path.isdir(self.path):
            return f'{self.path} was not written'
        self._scanned = self._scan()
        agg = self._aggregates(self._scanned)
        problem = self._check(agg['n_files'], agg['total_bytes'])
        return f'{self.path} {problem}' if problem else None

    def after_success(self, checksum=False):
        files = self._scanned if self._scanned is not None else self._scan()
        self._scanned = None
        agg = self._aggregates(files)
        if checksum or self.checksum:
            for rel, info in files.items():
                if not info['stale']:
                    info['sha256'] = sha256_file(os.path.join(self.path, rel))
        self._write_manifest(files, agg)
        if agg['n_stale']:
            logger.warning(
                '{}: {} file(s) ({} bytes) were not produced by this run and are '
                'recorded as stale; remake never deletes files, remove them by hand',
                self.path, agg['n_stale'], agg['stale_bytes'])
        return {'dir': True, 'n_files': agg['n_files'],
                'total_bytes': agg['total_bytes'], 'checksum': None}

    def _write_manifest(self, files, agg):
        data = {'version': 1, 'written': time.time(), 'started': self._start,
                **agg, 'files': files}
        tmp = self.manifest_path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, self.manifest_path)  # atomic: complete or absent

    def claim(self, write=True):
        """Adoption (`set-state --success --check-outputs`): a dir that
        exists without a manifest is scanned and everything present is
        claimed as produced. True if the dir is (now) complete. With
        `write=False`, only report whether it would be."""
        if self.is_complete():
            return True
        if self.manifest() is not None or not os.path.isdir(self.path):
            return False  # a manifest that fails Ensure: don't adopt
        files = self._scan(claim_all=True)
        agg = self._aggregates(files)
        if self._check(agg['n_files'], agg['total_bytes']) and self._ensure_requested():
            return False
        if write:
            self._start = None
            self._write_manifest(files, agg)
        return True


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
