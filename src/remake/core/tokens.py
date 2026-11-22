"""Output tokens.

Task completion is tracked in the metadata DB; tokens are the opt-in
verification layer — a way to ask the world, rather than the DB, whether an
output exists in finished form. Each token type encodes its own definition
of *finished*.

Path-backed tokens are transparent (`os.PathLike`): rule code passes them
straight to open(), Path(), xarray, zarr — no unwrapping.

The interface below (`OutputToken`) is frozen at 1.0 (0.10 item 5a):
`identity`, `is_complete`, `format`, `validate`, `before_run`,
`after_success`, `stat`, `complete_many`, `canonical`, `adopt` and the
`remote` flag. `str(token) == token.identity()`, and a `PathToken`'s
`.path` / `__fspath__` are part of it.
"""
import abc
import copy
import hashlib
import json
import os
import unicodedata
import re
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from loguru import logger


def sha256_file(path, bufsize=1 << 20):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True, slots=True)
class OutputStat:
    """What `OutputToken.stat()` reports, without reading the data.

    exists: something is there (not the same as complete: a Dir without its
      manifest exists but is not complete).
    size: bytes, or None if unknown or not cheap to know.
    mtime: modification time (seconds since the epoch), or None.

    A dataclass, not a tuple, so fields can be added after 1.0."""
    exists: bool
    size: int | None = None
    mtime: float | None = None


_URL_RE = re.compile(r'^[A-Za-z][A-Za-z0-9+.-]+:/')


@lru_cache(maxsize=65536)
def _real_dir(directory):
    return os.path.realpath(directory)


def canonical_path(path):
    """The one canonical spelling of a local path (0.10 item 5a): absolute,
    normalised, with symlinks in its *directory* resolved (one realpath per
    distinct directory, cached). A symlinked final component is kept as is,
    so a link output is the link, not its target. Used by lint, the
    missing-inputs check and Dir ownership. Not (yet) by dependency matching
    between tasks (deps._norm, planner._norm_paths compare normpath'd
    strings), where two spellings of one file make a task depend on every
    upstream task: conservative, never a missed rerun."""
    absolute = os.path.abspath(os.fspath(path))
    directory, name = os.path.split(absolute)
    return os.path.join(_real_dir(directory), name) if name else _real_dir(directory)


def _fold(name):
    """A file name as a case-insensitive, normalising filesystem compares it."""
    return unicodedata.normalize('NFC', name).casefold()


def dir_keys(path):
    """The spellings a Dir output is matched under: `canonical_path` (the
    link, if the Dir is a symlink) and its full realpath (the target). A Dir
    symlinked to scratch must still contain the files read through it,
    whose canonical form resolves the link (review 2026-11-22, finding 1)."""
    absolute = os.path.abspath(os.fspath(path))
    return {canonical_path(absolute), os.path.realpath(absolute)}


def canonical(value):
    """The canonical identity of an input or output value: a token's
    `canonical()`, a URL string unchanged, a local path `canonical_path`."""
    if isinstance(value, OutputToken):
        return value.canonical()
    text = os.fspath(value) if isinstance(value, os.PathLike) else str(value)
    if _URL_RE.match(text):
        return text
    return canonical_path(text)


def is_remote(value):
    """Is this input/output remote (an S3 object, a URL)? Remote values are
    skipped by remake's implicit plan-time checks: missing inputs, lint, Dir
    ownership, creating parent directories. Explicit checks (check_outputs,
    `remake verify`) still ask them."""
    if isinstance(value, OutputToken):
        return value.remote
    text = os.fspath(value) if isinstance(value, os.PathLike) else str(value)
    return bool(_URL_RE.match(text))


def complete_many(tokens):
    """[token.is_complete() for token in tokens], batched: tokens are grouped
    by exact class and each group goes through that class's `complete_many`
    (local paths: one directory listing per directory instead of one stat per
    file). The order of the results matches `tokens`."""
    tokens = list(tokens)
    results = [None] * len(tokens)
    groups = {}
    for i, token in enumerate(tokens):
        groups.setdefault(type(token), []).append(i)
    for cls, indices in groups.items():
        answers = cls.complete_many([tokens[i] for i in indices])
        for i, answer in zip(indices, answers):
            results[i] = bool(answer)
    return results


class OutputToken(abc.ABC):
    # Output validation options (0.9.0 item 4), set by `Ensure(...)`. Not part
    # of the token's identity or repr, so adding or changing validation is
    # not an inputs/outputs change (no rerun). Part of the frozen interface:
    # a custom validate() reads them.
    nonempty = False
    min_size = None
    checksum = False
    # Remote outputs (S3, ...) are skipped by plan-time checks; see is_remote.
    remote = False

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
        output — {'n_files', 'total_bytes', 'checksum'} (any may be None),
        plus 'aggregate': True for a token summarising many files (a Dir),
        which is then recorded under its own name rather than summed with
        the plain files — or None for nothing to record. `checksum`: the
        rule/global `checksum_outputs` opt-in; `Ensure(checksum=True)` also
        turns it on."""
        return None

    def stat(self):
        """Existence (and size/mtime where cheap) without reading the data.
        Default: exists = is_complete(); override for something cheaper or
        richer."""
        return OutputStat(exists=self.is_complete())

    @classmethod
    def complete_many(cls, tokens):
        """is_complete() for many tokens of this class at once, in order.
        Default: a loop. Override where a batch is cheaper (one listing, one
        API call). Call the module-level `complete_many`, which groups mixed
        token types."""
        return [token.is_complete() for token in tokens]

    def canonical(self):
        """The canonical identity used to compare outputs with inputs and
        with each other (not for display, and not hashed: identity() is).
        Default: identity()."""
        return self.identity()

    def adopt(self, write=True):
        """Adoption (`remake verify --adopt`, `set-state --success
        --check-outputs`): may this existing output be recorded as produced
        by its task? Default: is_complete(). A token whose completion is a
        marker remake writes (Dir) writes it here, unless write=False."""
        return self.is_complete()

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

    def canonical(self):
        return canonical_path(self.path)

    def format(self, **kwargs):
        # A copy, so Ensure options (and any subclass state) carry over.
        new = copy.copy(self)
        new.path = self.path.format(**kwargs)
        return new

    def _marker_path(self):
        """The one file whose existence means complete (no Ensure checks)."""
        return self.path

    # Below this many tokens in one directory, a stat each beats listing it
    # (a 1e6-entry directory listed to find one file).
    _SCAN_THRESHOLD = 8

    @classmethod
    def complete_many(cls, tokens):
        """Local paths: one os.scandir per directory holding at least
        _SCAN_THRESHOLD markers, a stat each otherwise. Tokens with Ensure
        size checks, and classes that redefine is_complete, fall back to it."""
        tokens = list(tokens)
        if cls.is_complete not in _BATCHABLE_IS_COMPLETE:
            return [t.is_complete() for t in tokens]
        results = [None] * len(tokens)
        by_dir = {}
        for i, token in enumerate(tokens):
            if token.nonempty or token.min_size is not None:
                results[i] = token.is_complete()
                continue
            directory, name = os.path.split(os.path.normpath(token._marker_path()))
            by_dir.setdefault(directory, []).append((i, name))
        for directory, entries in by_dir.items():
            if len(entries) < cls._SCAN_THRESHOLD:
                for i, _ in entries:
                    results[i] = os.path.exists(tokens[i]._marker_path())
                continue
            try:
                with os.scandir(directory or '.') as it:
                    present = {e.name for e in it if not e.is_symlink() or os.path.exists(e.path)}
            except (FileNotFoundError, NotADirectoryError):
                present = set()
            except OSError:  # e.g. a search-only (--x) directory: stat each
                present = None
            # Folded names (case, Unicode normalisation): a name missing from
            # the listing is stat'd only if the directory holds one it could
            # be on a case-insensitive or normalising filesystem (review
            # 2026-11-22, finding 2). A plain miss, the common case when
            # planning never-run tasks, costs no stat (CI bench, 2026-11-22).
            if present is not None and not present:  # missing or empty dir
                for i, _ in entries:
                    results[i] = False
                continue
            folded = None  # built on the first miss only
            for i, name in entries:
                if present is not None and name in present:
                    results[i] = True
                    continue
                if present is not None:
                    if folded is None:
                        folded = {_fold(n) for n in present}
                    if _fold(name) not in folded:
                        results[i] = False
                        continue
                results[i] = os.path.exists(tokens[i]._marker_path())
        return results


class FileToken(PathToken):
    def stat(self):
        try:
            st = os.stat(self.path)
        except (FileNotFoundError, NotADirectoryError):
            return OutputStat(exists=False)
        return OutputStat(exists=True, size=st.st_size, mtime=st.st_mtime)

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
        if os.path.isdir(self.path):
            # A plain path the task made a directory (a .zarr written by
            # xarray, say): no file stats or checksum (use Dir for those);
            # never fail a task that succeeded (ultrareview 2026-11-21).
            return None
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
# Touched in the dir at task start: its mtime is the reference "produced"
# is judged against, stamped by the same file server (and at the same
# timestamp resolution) as the files the task writes, so a server clock
# behind the compute node's, or 1 s mtime granularity, can't make rewritten
# files look stale (code review 2026-11-21).
_START_MARKER = '.remake-started'


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

    def _marker_path(self):
        return self.manifest_path

    def stat(self):
        """exists: the directory is there; size: the manifest's total_bytes
        (None without a manifest)."""
        if not os.path.isdir(self.path):
            return OutputStat(exists=False)
        summary = self.summary()
        mtime = None
        try:
            mtime = os.stat(self.manifest_path).st_mtime
        except OSError:
            pass
        return OutputStat(exists=True,
                          size=summary['total_bytes'] if summary else None, mtime=mtime)

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
                if rel in (MANIFEST_NAME, MANIFEST_NAME + '.tmp', _START_MARKER):
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
        marker = os.path.join(self.path, _START_MARKER)
        try:
            with open(marker, 'w'):
                pass
            self._start = os.stat(marker).st_mtime
            os.remove(marker)
        except OSError:
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
        return {'aggregate': True, 'n_files': agg['n_files'],
                'total_bytes': agg['total_bytes'], 'checksum': None}

    def _write_manifest(self, files, agg):
        data = {'version': 1, 'written': time.time(), 'started': self._start,
                **agg, 'files': files}
        tmp = self.manifest_path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, self.manifest_path)  # atomic: complete or absent

    def claim(self, write=True):
        """Deprecated alias of adopt() (0.9.0 name)."""
        return self.adopt(write=write)

    def adopt(self, write=True):
        """Adoption: a dir that exists without a manifest is scanned and
        everything present is claimed as produced. True if the dir is (now)
        complete. With `write=False`, only report whether it would be."""
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

    def _marker_path(self):
        return os.path.join(self.path, '.zmetadata')

    def stat(self):
        return OutputStat(exists=os.path.isdir(self.path))


class S3Object(OutputToken):
    """An object in S3. Experimental: storage backends are planned for after
    1.0, so this class is not covered by the 1.0 compatibility promise."""

    remote = True

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
    if type(token).validate is OutputToken.validate:
        # ZarrStore, S3Object, ...: nothing would run the checks, and a
        # silent no-op is worse than an error (ultrareview 2026-11-21).
        raise TypeError(
            f'Ensure() cannot check a {type(token).__name__} output: it supports '
            f'file paths and Dir (or a token class that implements validate())')
    token.nonempty = nonempty
    token.min_size = min_size
    token.checksum = checksum
    return token


# The is_complete implementations that are a plain marker-existence test, so
# the PathToken batch (a listing) is equivalent. A subclass inherits the
# batch only if it keeps one of these; one that redefines is_complete (or
# Dir, which reads its manifest) falls back to calling it per token.
_BATCHABLE_IS_COMPLETE = (FileToken.is_complete, ZarrStore.is_complete)


def as_token(value):
    """Wrap plain strings/Paths in FileToken; pass tokens through."""
    if isinstance(value, OutputToken):
        return value
    if isinstance(value, (str, Path)):
        return FileToken(value)
    raise TypeError(f'Cannot make an output token from {value!r}')
