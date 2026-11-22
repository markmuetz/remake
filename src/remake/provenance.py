"""Provenance capture (0.10 item 1): what a run ran with, recorded per
invocation on the `run` row and never compared (it is not a rerun trigger).

- git: commit, dirty flag, and a hash of `git diff HEAD` when dirty, for the
  repo holding the remakefile (decision D2). The diff itself is not stored.
- environment: Python and remake versions, plus a fingerprint of the
  installed distributions read with `importlib.metadata` (no subprocess),
  and the hash of a lockfile if one is found (D1).
- host and user: hostname in plain text, the username only as a hash (D4).
  The hash is unsalted, so it is pseudonymous, not anonymous: anyone with
  a list of candidate usernames can match it.

Everything here is best effort: a missing `git`, a directory outside a
repo, or an unreadable distribution gives "not recorded", never an error.
"""
import getpass
import hashlib
import json
import platform
import socket
import subprocess
from functools import lru_cache
from importlib import metadata as importlib_metadata
from pathlib import Path

from .version import __version__

LOCKFILES = ('uv.lock', 'pixi.lock', 'conda-lock.yml', 'poetry.lock')
GIT_TIMEOUT_S = 5


def _sha256(data):
    """Hex sha256 of bytes or str."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()


def _git(directory, *args):
    """stdout of `git -C directory args...`, or None on any failure."""
    try:
        # --no-optional-locks: never take index.lock (a `git commit` running
        # alongside must not collide with us).
        proc = subprocess.run(
            ['git', '--no-optional-locks', '-C', str(directory), *args], capture_output=True,
            timeout=GIT_TIMEOUT_S, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def git_state(directory, remakefile=None):
    """{'commit', 'dirty', 'diff_hash'} for the repo holding `directory`, or
    {} if it isn't in one (or git is unavailable). One `git status` call
    (plus `git diff` only when dirty). Untracked files don't make the tree
    dirty, except the remakefile itself: a pipeline that was never
    committed is not "at" HEAD, and its content goes into the diff hash."""
    out = _git(directory, 'status', '--porcelain=v2', '--branch',
               '--untracked-files=no')
    if out is None:
        return {}
    commit, dirty = None, False
    for line in out.decode(errors='replace').splitlines():
        if line.startswith('# branch.oid '):
            commit = line.split(' ', 2)[2].strip()
        elif not line.startswith('#'):
            dirty = True
    if commit in (None, '(initial)'):
        return {}  # a repo with no commits yet
    untracked = None
    if remakefile is not None:
        name = Path(remakefile).name
        if _git(directory, 'ls-files', '--error-unmatch', '--', name) is None:
            try:
                untracked = Path(directory, name).read_bytes()
                dirty = True
            except OSError:
                untracked = None
    diff_hash = None
    if dirty:
        # --no-ext-diff/--no-textconv: hash git's own diff, never the output
        # of a user-configured program.
        diff = _git(directory, 'diff', '--no-ext-diff', '--no-textconv', 'HEAD')
        if diff is not None:
            diff_hash = _sha256(diff + (b'\0untracked remakefile\0' + untracked
                                        if untracked is not None else b''))
    return {'commit': commit, 'dirty': dirty, 'diff_hash': diff_hash}


def installed_distributions():
    """Sorted, de-duplicated `name==version` strings for every installed
    distribution visible to this interpreter. Names are normalised (PEP 503)
    so the same environment always gives the same list; where a name appears
    twice on sys.path, the first (the one imported) wins."""
    seen = {}
    for dist in importlib_metadata.distributions():
        try:
            name = dist.metadata['Name']
            version = dist.version
        except Exception:  # broken metadata: skip, don't fail the run
            continue
        if not name:
            continue
        key = name.lower().replace('_', '-').replace('.', '-')
        seen.setdefault(key, f'{key}=={version}')
    return sorted(seen.values())


def environment_fingerprint(packages=None):
    """(fingerprint, packages): a sha256 over the Python version and the
    installed distributions. Two processes with the same fingerprint import
    the same versions of everything."""
    if packages is None:
        packages = installed_distributions()
    text = json.dumps({'python': platform.python_version(), 'packages': packages})
    return _sha256(text), packages


@lru_cache(maxsize=None)
def current_environment():
    """This process's (fingerprint, packages), computed once: the scan costs
    tens of ms, so it is done lazily and only where it is recorded."""
    fingerprint, packages = environment_fingerprint()
    return fingerprint, tuple(packages)


def find_lockfile(directory, in_repo=True):
    """The nearest lockfile in `directory` or, inside a git repo, a parent
    up to the repo root (a directory holding `.git`): (path, sha256) or
    (None, None). Outside a repo only `directory` itself is looked at, so an
    unrelated ~/uv.lock is never recorded."""
    directory = Path(directory).resolve()
    for d in (directory, *(directory.parents if in_repo else ())):
        for name in LOCKFILES:
            candidate = d / name
            if candidate.is_file():
                try:
                    return candidate, _sha256(candidate.read_bytes())
                except OSError:
                    return candidate, None
        if (d / '.git').exists():
            break
    return None, None


def host_and_user():
    """(hostname, sha256 of the username), D4. The username is hashed so an
    archived `.remake/` doesn't carry it in plain text; the hash still lets
    two runs be attributed to the same person."""
    try:
        host = socket.gethostname()
    except OSError:
        host = None
    try:
        user = getpass.getuser()
    except Exception:  # no login name (containers, some batch systems)
        user = None
    return host, (_sha256(user)[:16] if user else None)


def collect(directory, remakefile=None):
    """Everything recorded on a `run` row, for a remakefile in `directory`."""
    git = git_state(directory, remakefile)
    fingerprint, packages = current_environment()
    lockfile, lock_hash = find_lockfile(directory, in_repo=bool(git))
    host, user_hash = host_and_user()
    return {
        'git_commit': git.get('commit'),
        'git_dirty': git.get('dirty'),
        'git_diff_hash': git.get('diff_hash'),
        'python_version': platform.python_version(),
        'remake_version': __version__,
        'env_fingerprint': fingerprint,
        'env_packages': list(packages),
        'lockfile': lockfile.name if lockfile else None,
        'lockfile_hash': lock_hash,
        'host': host,
        'user_hash': user_hash,
    }
