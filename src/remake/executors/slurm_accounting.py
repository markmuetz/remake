"""SLURM accounting (sacct): what happened to array elements that have left
the queue. Used by reconcile (design_docs/designs/slurm_reconcile/plan.md
§4) to tell a killed task from one that never started.

Formats are taken from real output: the soak archive and a live probe on
JASMIN (tests/slurm/fixtures/). The rules that matter:

- Only an element's **own** row with a Start time is evidence that it ran.
  Rows without a start are an array's unsplit pending remainder, labelled
  with a range (`J_[0,2-3]`) or, for aftercorr arrays, with one index
  (`J_5` stood for elements 0, 2, 3 and 5): evidence about no element in
  particular, so they are kept apart (`JobAccounting.unsplit`).
- Never-started elements often have no row at all (aftercorr-cancelled,
  or cancelled singly while pending).
- `CANCELLED by <uid>`: the state is the first word.
"""
import datetime as dt
import os
import re
import subprocess as sp
import time
from dataclasses import dataclass, field

from ..core.exceptions import RemakeError

SACCT_FIELDS = ('JobID', 'State', 'ExitCode', 'Start', 'End', 'Elapsed', 'NodeList')
SACCT_TIMEOUT = 60
# Job ids per sacct call (a long -j list makes one huge command line).
SACCT_CHUNK = 200

# States after which an element never runs again.
TERMINAL_STATES = frozenset((
    'COMPLETED', 'FAILED', 'CANCELLED', 'NODE_FAIL', 'TIMEOUT', 'OUT_OF_MEMORY',
    'PREEMPTED', 'BOOT_FAIL', 'DEADLINE', 'REVOKED',
))
# States in which an element may still run (or is running).
ACTIVE_STATES = frozenset((
    'PENDING', 'RUNNING', 'REQUEUED', 'REQUEUE_HOLD', 'REQUEUE_FED', 'RESIZING',
    'SUSPENDED', 'COMPLETING', 'CONFIGURING', 'STAGE_OUT', 'SIGNALING',
))


class SacctError(RemakeError):
    """sacct could not be run or its output not read: accounting is
    unknown (never "nothing happened")."""


@dataclass(frozen=True)
class Element:
    """One sacct row: an array element, or an unsplit remainder."""
    state: str
    start: int | None  # epoch seconds; None = not started
    end: int | None
    elapsed: int  # seconds
    node: str | None  # None = 'None assigned'
    exit_code: str

    @property
    def started(self):
        return self.start is not None and self.node is not None


@dataclass
class JobAccounting:
    """sacct's rows for one array job."""
    elements: dict = field(default_factory=dict)  # index -> Element (started rows)
    unsplit: list = field(default_factory=list)  # [(indices or None, Element)]

    def element(self, index):
        return self.elements.get(int(index))


def _epoch(text):
    text = text.strip()
    if not text or text in ('Unknown', 'None', 'N/A'):
        return None
    if text.isdigit():
        return int(text)
    # ISO local time (no SLURM_TIME_FORMAT): the cluster's local zone.
    return int(time.mktime(dt.datetime.fromisoformat(text).timetuple()))


def _seconds(text):
    """sacct Elapsed: [D-]HH:MM:SS, MM:SS or SS."""
    text = text.strip()
    days = 0
    if '-' in text:
        d, text = text.split('-', 1)
        days = int(d)
    parts = [int(p) for p in text.split(':')]
    while len(parts) < 3:
        parts.insert(0, 0)
    h, m, s = parts
    return ((days * 24 + h) * 60 + m) * 60 + s


def _indices(spec):
    """'0-3,7%4' -> [0, 1, 2, 3, 7]."""
    spec = spec.split('%')[0]
    out = []
    for part in spec.split(','):
        a, _, b = part.partition('-')
        out += list(range(int(a), int(b or a) + 1))
    return out


_JOBID = re.compile(r'^(\d+)_(?:\[([0-9,\-%]+)\]|(\d+))$')


def parse_sacct(text, fields=SACCT_FIELDS):
    """{base job id: JobAccounting} from `sacct -X -n -P` output with
    `fields` in that order. Rows that are not array elements (steps, plain
    jobs) are ignored; a malformed line raises SacctError."""
    idx = {name: i for i, name in enumerate(fields)}
    jobs = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        cols = line.split('|')
        if len(cols) < len(fields):
            raise SacctError(f'unexpected sacct line: {line!r}')
        m = _JOBID.match(cols[idx['JobID']].strip())
        if not m:
            continue  # a step (`J_1.batch`) or a non-array job
        base, bracket, single = m.groups()
        try:
            node = cols[idx['NodeList']].strip()
            el = Element(
                state=cols[idx['State']].split()[0] if cols[idx['State']].strip() else '',
                start=_epoch(cols[idx['Start']]),
                end=_epoch(cols[idx['End']]),
                elapsed=_seconds(cols[idx['Elapsed']]),
                node=None if node in ('', 'None assigned', '(null)') else node,
                exit_code=cols[idx['ExitCode']].strip(),
            )
        except ValueError as e:
            raise SacctError(f'unexpected sacct line: {line!r} ({e})') from e
        job = jobs.setdefault(base, JobAccounting())
        if bracket is not None:
            job.unsplit.append((_indices(bracket), el))
        elif el.started:
            job.elements[int(single)] = el
        else:
            # `J_5` without a start: an aftercorr array's remainder,
            # labelled with one of its indices (live probe).
            job.unsplit.append((None, el))
    return jobs


def sacct_snapshot(jobids, *, since=None, timeout=SACCT_TIMEOUT):
    """{base job id: JobAccounting} for the given array job ids, from one
    sacct call per chunk. `since`: epoch seconds of the earliest submission
    (sacct's -S; a day earlier, for clock skew). Jobs with no rows are
    absent. Raises SacctError when sacct is missing, fails, hangs or prints
    something unreadable."""
    jobids = sorted({str(j) for j in jobids}, key=int)
    env = {**os.environ, 'SLURM_TIME_FORMAT': '%s'}
    out = {}
    for i in range(0, len(jobids), SACCT_CHUNK):
        chunk = jobids[i:i + SACCT_CHUNK]
        cmd = ['sacct', '-X', '-n', '-P', '-j', ','.join(chunk),
               f'--format={",".join(SACCT_FIELDS)}']
        if since is not None:
            start = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(since - 86400))
            cmd[1:1] = ['-S', start]
        try:
            result = sp.run(cmd, capture_output=True, text=True, check=True,
                            timeout=timeout, env=env)
        except FileNotFoundError as e:
            raise SacctError('sacct not found: job accounting unavailable') from e
        except sp.TimeoutExpired as e:
            raise SacctError(f'sacct gave no response within {timeout}s') from e
        except sp.CalledProcessError as e:
            detail = (e.stderr or '').strip() or f'exit {e.returncode}'
            raise SacctError(f'sacct failed ({detail})') from e
        parsed = parse_sacct(result.stdout)
        out.update({k: v for k, v in parsed.items() if k in set(chunk)})
    return out
