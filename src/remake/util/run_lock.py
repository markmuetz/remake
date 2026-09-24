"""A per-directory lock so two local `remake run`s can't execute the same
tasks at once (review 2026-09-24 M15: both would run every task and race on
the same output files).

The lock file sits next to the metadata DB (`.remake/run.lock`) and records
the holder's host and PID. It is created atomically (O_EXCL) and removed when
the run ends, including on Ctrl-C. A lock left by a process that died on this
host is detected (its PID is gone) and taken over; one from another host (a
shared filesystem) can't be checked, so the error says how to clear it.
"""
import contextlib
import json
import os
import socket
from pathlib import Path

from loguru import logger

from ..core.exceptions import RemakeError


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


@contextlib.contextmanager
def run_lock(metadata):
    dbloc = getattr(metadata, 'dbloc', None)
    if not dbloc or dbloc == ':memory:':
        yield  # nothing on disk to protect (tests, custom backends)
        return
    path = Path(dbloc).parent / 'run.lock'
    me = {'host': socket.gethostname(), 'pid': os.getpid()}
    for attempt in (1, 2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                holder = json.loads(path.read_text())
            except (OSError, ValueError):
                holder = {}
            same_host = holder.get('host') == me['host']
            pid = holder.get('pid')
            if attempt == 1 and same_host and isinstance(pid, int) and not _pid_alive(pid):
                logger.warning(f'Removing stale run lock left by dead process {pid}: {path}')
                path.unlink(missing_ok=True)
                continue
            who = f'{holder.get("host", "?")} pid {holder.get("pid", "?")}'
            raise RemakeError(
                f'another remake run is active in this directory ({who}; lock '
                f'{path}). Wait for it to finish; if it is not running, delete '
                f'{path}'
            ) from None
        with os.fdopen(fd, 'w') as f:
            json.dump(me, f)
        break
    try:
        yield
    finally:
        path.unlink(missing_ok=True)
