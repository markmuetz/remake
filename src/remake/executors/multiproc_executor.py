"""Multiproc executor — parallel local execution on a process pool.

Same execution model as SLURM array elements, shrunk to one machine:
workers are fresh (spawned) processes that each load the remakefile once;
task specs travel as (rule_name, kwargs) and are rebuilt with
task_from_spec (rule functions can't be pickled); results are recorded as
sidecar files by the workers — no concurrent SQLite writers — and
ingested by the parent after each rule and by every plan().

Ordering: per-rule barriers. All tasks of a rule finish before the next
rule's start. Dependencies are rule-level (remake has no task DAG), so
this is exactly the ordering remake promises; the cost vs SLURM's
aftercorr is that same-matrix chains don't pipeline element-wise.

Per-task logs are written by the workers to the usual
.remake/tasks/log/<rule>/... locations.

Worker crashes (OOM kill, segfault, os._exit): the pool can't say which
task killed it, so each worker marks the task it is running
(RUNNING_ROOT/<key>) and clears the mark when the task ends. After a crash,
marked tasks were in flight — the culprit is among them — and are rerun one
at a time in a single-worker pool; a task whose worker dies there is
recorded as failed. Tasks that never started are resubmitted. Interrupts
(Ctrl-C, SIGTERM via the CLI) cancel queued work and terminate the workers.
(Review 2026-09-24 H7, M10.)
"""
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import get_context
from pathlib import Path

from loguru import logger

from ..core.exceptions import RemakeError
from ..core.deps import Edges
from ..core.planner import record_failure, upstream_failed
from ..metadata import TASK_STATUS_FAILED
from .executor import Executor

_worker_rmk = None

# In-flight markers: one empty file per task a worker is running.
RUNNING_ROOT = Path('.remake/tasks/running')

WORKER_DIED = (
    'The worker process running this task died without reporting a result — '
    'killed by the operating system (e.g. for using too much memory), or a '
    'crash in native code (segfault) or os._exit(). Re-run in isolation with '
    '`remake run-task` to investigate.'
)


def _default_nproc():
    """Usable CPU count. `os.sched_getaffinity` respects the cpuset/cgroup
    mask, so inside a SLURM allocation (`--cpus-per-task`) on a big shared
    node it returns the allocation, not the whole machine — unlike
    `os.cpu_count()`, which would oversubscribe. Falls back to `cpu_count()`
    where affinity is unavailable (non-Linux)."""
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count()


def _worker_init(remakefile):
    global _worker_rmk
    from ..loader import load_remake
    from ..metadata.sidecar import SidecarWriter

    logger.remove()  # workers log to per-task files only
    _worker_rmk = load_remake(remakefile, finalize=False)
    _worker_rmk.metadata = SidecarWriter()


def _worker_run(spec):
    from ..util import task_log_path

    rule_name, kwargs, run_seq = spec
    # Stamp results with the parent invocation's run_seq so durable rerun
    # propagation (bugs/01) works for multiproc runs as it does for SLURM.
    _worker_rmk.metadata.run_seq = run_seq
    task = _worker_rmk.task_from_spec(rule_name, kwargs)
    logfile = task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    marker = RUNNING_ROOT / task.key
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    sink_id = logger.add(logfile, level='DEBUG', mode='w')
    try:
        _worker_rmk.run_task(task)
        return True
    except Exception:
        return False  # recorded (sidecar + log) by run_task
    finally:
        logger.remove(sink_id)
        marker.unlink(missing_ok=True)


def _kill_pool(pool):
    """Stop a pool now: cancel queued work and terminate the workers (a plain
    shutdown would first let every queued task run to completion)."""
    procs = list((getattr(pool, '_processes', None) or {}).values())
    pool.shutdown(wait=False, cancel_futures=True)
    for proc in procs:
        if proc.is_alive():
            proc.terminate()
    for proc in procs:
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()


class MultiprocExecutor(Executor):
    def __init__(self, rmk, nproc=None):
        super().__init__(rmk)
        self.remakefile = rmk.remakefile
        if self.remakefile is None:
            raise RemakeError(
                'MultiprocExecutor needs the remakefile path (workers reload it): '
                'run via the remake CLI, or set rmk.remakefile'
            )
        self.nproc = (
            nproc or rmk.config.get('multiproc', {}).get('nproc') or _default_nproc()
        )

    def run_tasks(self, tasks):
        # Group consecutive same-rule tasks; plan order is rule-topological.
        groups = []
        for task in tasks:
            if groups and groups[-1][0] is task.rule:
                groups[-1][1].append(task)
            else:
                groups.append((task.rule, [task]))

        self._ntasks = len(tasks)
        self._done = 0
        nfailed = 0
        nskipped = 0
        failures = {}  # see planner.record_failure
        edges = Edges()  # which upstream tasks each task reads (upstream_failed)
        run_seq = self.rmk.metadata.current_run_seq()
        pool = self._new_pool(self.nproc)
        try:
            for rule, rule_tasks in groups:
                # The per-rule barrier means upstream failures are fully
                # known here: skip tasks they taint rather than running
                # them into missing inputs (left pending for a later run).
                to_run = []
                for task in rule_tasks:
                    if upstream_failed(task, failures, edges):
                        record_failure(failures, task)
                        nskipped += 1
                        self._done += 1
                        logger.warning(f'{self._done}/{self._ntasks} skipped (upstream failed): {task}')
                    else:
                        to_run.append(task)
                if not to_run:
                    continue
                logger.info(f'{rule.name}: {len(to_run)} task(s) on {self.nproc} proc(s)')
                pool, failed = self._run_rule(pool, rule, to_run, run_seq)
                for task in failed:
                    record_failure(failures, task)
                nfailed += len(failed)
        except BaseException:
            # Ctrl-C / SIGTERM (the CLI turns SIGTERM into KeyboardInterrupt)
            # or an unexpected error: don't let queued tasks run on, and
            # don't leave workers behind (review 2026-09-24 M10).
            _kill_pool(pool)
            logger.error(f'interrupted after {self._done}/{self._ntasks} task(s); '
                         f'workers stopped')
            raise
        pool.shutdown()
        if nfailed:
            skipped = f' ({nskipped} downstream task(s) skipped)' if nskipped else ''
            logger.error(f'{nfailed}/{self._ntasks} tasks failed{skipped}')
        # Workers wrote sidecars; fold them in so statuses are current
        # immediately (plan() would also pick them up on the next wave).
        self.rmk.metadata.ingest_sidecars(self.rmk.rules)
        return nfailed

    def _new_pool(self, nproc):
        return ProcessPoolExecutor(
            max_workers=nproc,
            mp_context=get_context('spawn'),
            initializer=_worker_init,
            initargs=(self.remakefile,),
        )

    def _submit(self, pool, rule, tasks, run_seq):
        for task in tasks:  # clear stale marks from an earlier, killed run
            (RUNNING_ROOT / task.key).unlink(missing_ok=True)
        return {pool.submit(_worker_run, (rule.name, t.kwargs, run_seq)): t
                for t in tasks}

    def _finish(self, task, ok, failed):
        self._done += 1
        if ok:
            logger.info(f'{self._done}/{self._ntasks}: {task}')
        else:
            failed.append(task)
            logger.error(f'{self._done}/{self._ntasks} failed: {task}')

    def _worker_died(self, task, failed):
        """Record a task whose worker died: no sidecar was written, so the
        parent records the failure itself."""
        self.rmk.metadata.update_task(task, TASK_STATUS_FAILED, exception=WORKER_DIED)
        self._done += 1
        failed.append(task)
        logger.error(f'{self._done}/{self._ntasks} failed (worker process died): {task}')

    def _run_rule(self, pool, rule, tasks, run_seq):
        """Run one rule's tasks to completion, surviving worker deaths.
        Returns (pool, failed tasks) — the pool may have been replaced."""
        failed = []
        pending = tasks
        while pending:
            futures = self._submit(pool, rule, pending, run_seq)
            unresolved = []
            # Barrier: drain this rule before starting the next.
            for future in as_completed(futures):
                task = futures[future]
                try:
                    ok = future.result()
                except BrokenProcessPool:
                    unresolved.append(task)
                    continue
                self._finish(task, ok, failed)
            if not unresolved:
                break
            # A worker died; the executor has torn the pool down.
            _kill_pool(pool)
            pool = self._new_pool(self.nproc)
            in_flight = [t for t in unresolved if (RUNNING_ROOT / t.key).exists()]
            pending = [t for t in unresolved if not (RUNNING_ROOT / t.key).exists()]
            if not in_flight:
                # Died before starting any task (e.g. the remakefile crashes
                # the interpreter on import): retrying would loop forever.
                logger.error(f'{rule.name}: worker processes are dying before '
                             f'running any task; giving up on {len(pending)} task(s)')
                for task in pending:
                    self._worker_died(task, failed)
                break
            logger.warning(
                f'{rule.name}: a worker process died with {len(in_flight)} task(s) '
                f'in flight; re-running those one at a time to find the cause'
            )
            self._isolate(rule, in_flight, run_seq, failed)
        return pool, failed

    def _isolate(self, rule, tasks, run_seq, failed):
        """Run tasks one at a time in a single-worker pool: a task whose
        worker dies here is the one that kills it."""
        solo = self._new_pool(1)
        try:
            for task in tasks:
                future = self._submit(solo, rule, [task], run_seq)
                try:
                    ok = next(iter(future)).result()
                except BrokenProcessPool:
                    (RUNNING_ROOT / task.key).unlink(missing_ok=True)
                    self._worker_died(task, failed)
                    _kill_pool(solo)
                    solo = self._new_pool(1)
                    continue
                self._finish(task, ok, failed)
        except BaseException:
            _kill_pool(solo)
            raise
        solo.shutdown()
