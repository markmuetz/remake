"""Per-task resource capture — wall time, CPU time and peak RSS.

Measured in `Remake.run_task`, the one execution chokepoint every executor
goes through, so all executors get the same numbers (design_docs/
resource_capture.md).

Peak RSS is the awkward one. `resource.getrusage().ru_maxrss` is a *process*
high-water mark that never decreases, so reading it at task end is only
correct when the process ran exactly one task. It does not in singleproc
(tasks run in the parent) or in multiproc/dask (pooled workers run many
tasks in sequence) — there it would report the largest task the process ever
ran, silently attributed to whichever task finished last. So:

- 'sample': a daemon thread reads /proc/self/statm every `interval`
  seconds and keeps the max, so an earlier task's *peak* is never charged
  to this one. Linux only; stdlib only (no psutil).
- 'rusage': no /proc, but the caller declares one task per process
  (`remake run-task`, `remake run-array-task`) — ru_maxrss at the end is
  the task's peak, to within the interpreter baseline.
- None: no /proc and a task-reusing process. Record nothing rather than a
  number that is wrong by construction.

`rss_method` travels with the number so consumers never compare a sampled
value against a getrusage one.

Known inaccuracies, deliberately not hidden:

- Sampling misses a spike shorter than `interval`.
- statm RSS counts shared pages, so shared libraries and page-cache-backed
  mmaps inflate it.
- The figure is the *process* RSS while the task ran, which has a floor: an
  earlier task in the same process may have left memory resident that the
  allocator never returned to the OS (CPython arenas commonly don't; large
  mmap'd buffers do). So a trivial task following a heavy one in a pooled
  worker can report the residual floor as its peak. This is the honest
  number for the question the measurement exists to answer — "how much
  memory must I request for a process running this task" — but it is a
  floor, not an attribution.
- Concurrent tasks in one process (a dask worker with threads_per_worker >
  1) cannot be told apart at all: statm and RUSAGE_SELF are process-wide.
  That case is detected and recorded as unmeasured rather than as N copies
  of the process total.
- A task killed by the OOM killer or by SLURM never returns here at all and
  records nothing (that is what the sacct audit is for).
"""
import os
import resource
import sys
import threading
from contextlib import contextmanager
from time import perf_counter

STATM_PATH = '/proc/self/statm'

# ru_maxrss is KiB on Linux, bytes on macOS/BSD. Normalise at the source;
# everything downstream of this module is bytes.
_MAXRSS_SCALE = 1 if sys.platform == 'darwin' else 1024


def _statm_rss_bytes():
    """Resident set size of this process from /proc, or None if unreadable."""
    try:
        with open(STATM_PATH) as f:
            pages = int(f.read().split()[1])
    except (OSError, IndexError, ValueError):
        return None
    return pages * os.sysconf('SC_PAGE_SIZE')


def _rusage_maxrss_bytes(who):
    return resource.getrusage(who).ru_maxrss * _MAXRSS_SCALE


def _cpu_seconds():
    """CPU time (user+sys) of this process and its waited-for children."""
    total = 0.0
    for who in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN):
        ru = resource.getrusage(who)
        total += ru.ru_utime + ru.ru_stime
    return total


# Minimum sampling period. `rss_interval` is a user knob, and 0 would turn
# `Event.wait` into a busy loop that burns a core reading /proc — charged, via
# the process-wide RUSAGE_SELF, to the task's own cpu_s, corrupting the very
# number users size allocations from.
MIN_INTERVAL = 0.001


def _clean_interval(value, default=0.1):
    """A usable sampling period from user config: numeric, and not so small
    that the sampler becomes a spin loop."""
    try:
        interval = float(value)
    except (TypeError, ValueError):
        return default
    if interval != interval or interval <= 0:  # NaN or nonsense
        return default
    return max(interval, MIN_INTERVAL)


class _RssSampler:
    """Background sampler of /proc/self/statm; `peak` is the max seen."""

    def __init__(self, interval):
        self.interval = interval
        self.peak = _statm_rss_bytes() or 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def _sample(self):
        rss = _statm_rss_bytes()
        if rss is not None and rss > self.peak:
            self.peak = rss

    def start(self):
        """Start sampling; False if no thread could be started. Measurement
        must never be what breaks a run, so a thread-limited machine
        (`ulimit -u`, memory pressure) degrades to no RSS figure."""
        try:
            self._thread.start()
        except RuntimeError:
            return False
        return True

    def stop(self):
        # Force a final sample: a task shorter than one interval would
        # otherwise be described only by the reading taken before it ran.
        self._sample()
        self._stop.set()
        self._thread.join(timeout=self.interval + 1)
        return self.peak


# Captures currently running in this process. Two overlapping captures make
# both unattributable: statm and RUSAGE_SELF measure the process, not the
# task. A capture that starts while others are active taints them and itself
# — the overlap can begin at any point in either lifetime, so the flag is set
# on entry of the *later* capture rather than checked once.
_ACTIVE_CAPTURES = set()
_ACTIVE_LOCK = threading.Lock()


def _enter_capture(capture):
    with _ACTIVE_LOCK:
        if _ACTIVE_CAPTURES:
            capture._shared_process = True
            for other in _ACTIVE_CAPTURES:
                other._shared_process = True
        _ACTIVE_CAPTURES.add(capture)


def _leave_capture(capture):
    """Deregister; True if this capture ever overlapped another."""
    with _ACTIVE_LOCK:
        _ACTIVE_CAPTURES.discard(capture)
        return capture._shared_process


class ResourceCapture:
    """Context manager measuring one task's resource use.

    `.result()` returns the dict recorded against the task
    (`wall_s`/`cpu_s`/`max_rss_bytes`/`rss_method`); it is valid inside the
    `with` body's `except` handler too, so a failing task is measured as
    well as a succeeding one — a task that fails after three hours is the
    most valuable duration in the DB.

    :param interval: RSS sampling period in seconds.
    :param sample_rss: False disables the sampler thread (config knob);
        wall and CPU time are free and always measured.
    :param one_task_per_process: True when the caller knows this process
        runs a single task, which makes the getrusage fallback valid.
    """

    def __init__(self, interval=0.1, sample_rss=True, one_task_per_process=False):
        self.interval = _clean_interval(interval)
        self.sample_rss = sample_rss
        self.one_task_per_process = one_task_per_process
        self._sampler = None
        self._start_wall = perf_counter()
        self._start_cpu = 0.0
        self._start_children_rss = 0
        self._shared_process = False
        self._result = {
            'wall_s': None, 'cpu_s': None, 'max_rss_bytes': None, 'rss_method': None}

    def __enter__(self):
        # Nothing here may raise: a failed measurement must not take the task
        # with it (and run_task's failure handler needs a usable result).
        self._start_cpu = _cpu_seconds()
        self._start_children_rss = _rusage_maxrss_bytes(resource.RUSAGE_CHILDREN)
        _enter_capture(self)
        try:
            if self.sample_rss and _statm_rss_bytes() is not None:
                sampler = _RssSampler(self.interval)
                if sampler.start():
                    self._sampler = sampler
        except Exception:
            # Belt and braces around the one part that touches OS limits: if
            # setting up measurement fails at all, run the task unmeasured.
            # An escape here would also leave this capture registered as
            # active forever, tainting every later task in the process.
            self._sampler = None
        self._start_wall = perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        wall = perf_counter() - self._start_wall
        cpu = _cpu_seconds() - self._start_cpu
        if self._sampler is not None:
            self._sampler.stop()
        # Another capture overlapped this one in the same process (a dask
        # worker with threads_per_worker > 1): statm and RUSAGE_SELF are
        # process-wide, so every concurrent task would record the process
        # total and label it per-task. Record wall time — which is still this
        # task's — and nothing else, per "no number beats a wrong one".
        if _leave_capture(self):
            self._result.update(wall_s=wall, cpu_s=None,
                                max_rss_bytes=None, rss_method=None)
            return False
        self._result['wall_s'] = wall
        # Clamp: getrusage has coarse granularity, and a task that reaps a
        # child started before it can in principle show a small negative.
        self._result['cpu_s'] = max(cpu, 0.0)
        self._result.update(self._peak_rss())
        return False  # never swallow the task's exception

    def _peak_rss(self):
        # `capture: False` means "don't measure memory", not "measure it some
        # other way": the getrusage fallback below is for when sampling was
        # wanted but /proc was unavailable (or no thread could be started).
        if not self.sample_rss:
            return {'max_rss_bytes': None, 'rss_method': None}
        # A task that shells out (cdo, ncks — normal in this user base) does
        # its allocating in a child, which statm never sees. RUSAGE_CHILDREN
        # is also a high-water mark, so it only means something here when it
        # *grew* during the task.
        children = _rusage_maxrss_bytes(resource.RUSAGE_CHILDREN)
        grew = children if children > self._start_children_rss else None
        if self._sampler is not None:
            peak = self._sampler.peak  # stopped (final sample taken) in __exit__
            if grew is not None:
                peak = max(peak, grew)
            return {'max_rss_bytes': peak, 'rss_method': 'sample'}
        if self.one_task_per_process:
            peak = _rusage_maxrss_bytes(resource.RUSAGE_SELF)
            if grew is not None:
                peak = max(peak, grew)
            return {'max_rss_bytes': peak, 'rss_method': 'rusage'}
        return {'max_rss_bytes': None, 'rss_method': None}

    def result(self):
        """The measurement so far as a dict (complete once the block exits)."""
        return dict(self._result)


# Set by `remake run-task`/`run-array-task`: this process runs exactly one
# task, so the getrusage fallback is valid where /proc is unavailable. A
# module-level flag rather than an argument threaded through every executor —
# it is a property of the process, not of the call.
ONE_TASK_PER_PROCESS = False


@contextmanager
def one_task_per_process():
    """Declare that this process runs a single task (the per-task CLI entry
    points). Restores the previous value on exit: those commands are their
    own process in the field, but the CLI is also called in-process (tests,
    library use), where a permanently-flipped global would silently license
    the getrusage fallback for every later task in that process."""
    global ONE_TASK_PER_PROCESS
    previous = ONE_TASK_PER_PROCESS
    ONE_TASK_PER_PROCESS = True
    try:
        yield
    finally:
        ONE_TASK_PER_PROCESS = previous


def capture_for_config(config):
    """Build a `ResourceCapture` from a Remake config's `resources` block:

        config={'resources': {'capture': True, 'rss_interval': 0.1}}

    `capture` (default True) turns the RSS sampler on/off; wall and CPU time
    are free and always measured.
    """
    cfg = (config or {}).get('resources', {})
    return ResourceCapture(
        interval=cfg.get('rss_interval', 0.1),
        sample_rss=cfg.get('capture', True),
        one_task_per_process=ONE_TASK_PER_PROCESS,
    )
