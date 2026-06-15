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
"""
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import get_context

from loguru import logger

from ..core.exceptions import RemakeError
from ..core.planner import upstream_failed
from .executor import Executor

_worker_rmk = None


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

    rule_name, kwargs = spec
    task = _worker_rmk.task_from_spec(rule_name, kwargs)
    logfile = task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    sink_id = logger.add(logfile, level='DEBUG', mode='w')
    try:
        _worker_rmk.run_task(task)
        return True
    except Exception:
        return False  # recorded (sidecar + log) by run_task
    finally:
        logger.remove(sink_id)


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

        ntasks = len(tasks)
        nfailed = 0
        nskipped = 0
        done = 0
        failures = {}  # rule -> set of frozenset(kwargs.items())
        with ProcessPoolExecutor(
            max_workers=self.nproc,
            mp_context=get_context('spawn'),
            initializer=_worker_init,
            initargs=(self.remakefile,),
        ) as pool:
            for rule, rule_tasks in groups:
                # The per-rule barrier means upstream failures are fully
                # known here: skip tasks they taint rather than running
                # them into missing inputs (left pending for a later run).
                to_run = []
                for task in rule_tasks:
                    if upstream_failed(task, failures):
                        failures.setdefault(rule, set()).add(frozenset(task.kwargs.items()))
                        nskipped += 1
                        done += 1
                        logger.warning(f'{done}/{ntasks} skipped (upstream failed): {task}')
                    else:
                        to_run.append(task)
                if not to_run:
                    continue
                logger.info(f'{rule.name}: {len(to_run)} task(s) on {self.nproc} proc(s)')
                futures = {
                    pool.submit(_worker_run, (rule.name, t.kwargs)): t for t in to_run
                }
                # Barrier: drain this rule before starting the next.
                for future in as_completed(futures):
                    done += 1
                    task = futures[future]
                    if future.result():
                        logger.info(f'{done}/{ntasks}: {task}')
                    else:
                        failures.setdefault(rule, set()).add(frozenset(task.kwargs.items()))
                        nfailed += 1
                        logger.error(f'{done}/{ntasks} failed: {task}')
        if nfailed:
            skipped = f' ({nskipped} downstream task(s) skipped)' if nskipped else ''
            logger.error(f'{nfailed}/{ntasks} tasks failed{skipped}')
        # Workers wrote sidecars; fold them in so statuses are current
        # immediately (plan() would also pick them up on the next wave).
        self.rmk.metadata.ingest_sidecars(self.rmk.rules)
        return nfailed
