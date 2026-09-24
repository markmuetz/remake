"""Dask executor — parallel execution on a dask.distributed cluster.

Same execution model as multiproc/SLURM: task specs travel as
(rule_name, kwargs); workers load the remakefile once (cached per worker
process) and rebuild tasks with task_from_spec; results are recorded as
sidecar files — dask workers may be on other machines, so they never
touch the shared SQLite DB — and ingested by the parent after each rule.
Per-rule barriers, with upstream-failure skipping, as in multiproc.

By default a LocalCluster is started per run_tasks call (one per wave for
dynamic pipelines — acceptable for now). Point at an existing cluster
with Remake(config={'dask': {'scheduler': 'tcp://...'}}); remote workers
must share the filesystem and working directory (the same contract as
SLURM jobs), and have remake + the pipeline's deps importable.

Long-lived workers cache the loaded remakefile: editing it mid-run is
not picked up until the workers restart.

`distributed` is an optional dependency: pip install remake[dask].
"""
import os

from loguru import logger

from ..core.exceptions import RemakeError
from ..core.planner import record_failure, upstream_failed
from ..metadata import TASK_STATUS_FAILED
from .multiproc_executor import RUNNING_ROOT, WORKER_DIED
from .executor import Executor

_worker_rmk_cache = {}


def _run_spec(remakefile, rule_name, kwargs, run_seq=None):
    """Runs on a dask worker. Returns True on success."""
    from ..loader import load_remake
    from ..metadata.sidecar import SidecarWriter
    from ..util import task_log_path

    rmk = _worker_rmk_cache.get(remakefile)
    if rmk is None:
        rmk = load_remake(remakefile, finalize=False)
        rmk.metadata = SidecarWriter()
        _worker_rmk_cache[remakefile] = rmk
    # The parent invocation's run_seq (durable propagation, bugs/01); set per
    # call because long-lived workers outlive a single invocation.
    rmk.metadata.run_seq = run_seq
    task = rmk.task_from_spec(rule_name, kwargs)
    logfile = task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    # In-flight marker, as in multiproc: tells the parent whether a task the
    # cluster failed (KilledWorker) had actually started.
    marker = RUNNING_ROOT / task.key
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    sink_id = logger.add(logfile, level='DEBUG', mode='w')
    try:
        rmk.run_task(task)
        return True
    except Exception:
        return False  # recorded (sidecar + log) by run_task
    finally:
        logger.remove(sink_id)
        marker.unlink(missing_ok=True)


class DaskExecutor(Executor):
    def __init__(self, rmk, nproc=None, scheduler=None):
        super().__init__(rmk)
        self.remakefile = rmk.remakefile
        if self.remakefile is None:
            raise RemakeError(
                'DaskExecutor needs the remakefile path (workers reload it): '
                'run via the remake CLI, or set rmk.remakefile'
            )
        config = rmk.config.get('dask', {})
        self.scheduler = scheduler or config.get('scheduler')
        self.nproc = nproc or config.get('nproc') or os.cpu_count()

    def _client(self):
        try:
            from distributed import Client, LocalCluster
        except ImportError:
            raise RemakeError(
                'The dask executor needs distributed: pip install remake[dask]'
            )
        if self.scheduler:
            return Client(self.scheduler), None
        import dask

        # A task that kills its worker (OOM, segfault) fails at once as
        # KilledWorker instead of being retried on 3 more workers — each
        # retry would kill another (review 2026-09-24 H7). The scheduler
        # reads this at start-up, so it only applies to clusters we create.
        # worker-saturation 1.0: a single-threaded worker holds only the task
        # it is running; the rest wait on the scheduler, so a dying worker
        # takes no queued, never-started tasks down with it.
        with dask.config.set({'distributed.scheduler.allowed-failures': 0,
                              'distributed.scheduler.worker-saturation': 1.0}):
            cluster = LocalCluster(
                n_workers=self.nproc, threads_per_worker=1, dashboard_address=None
            )
        return Client(cluster), cluster

    def run_tasks(self, tasks):
        from distributed import as_completed

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
        failures = {}  # see planner.record_failure
        run_seq = self.rmk.metadata.current_run_seq()
        client, cluster = self._client()
        futures = {}
        try:
            for rule, rule_tasks in groups:
                to_run = []
                for task in rule_tasks:
                    if upstream_failed(task, failures):
                        record_failure(failures, task)
                        nskipped += 1
                        done += 1
                        logger.warning(f'{done}/{ntasks} skipped (upstream failed): {task}')
                    else:
                        to_run.append(task)
                if not to_run:
                    continue
                logger.info(f'{rule.name}: {len(to_run)} task(s) on dask ({self.nproc} workers)')
                pending, attempts = to_run, 0
                while pending:
                    attempts += 1
                    for task in pending:  # clear stale marks from a killed run
                        (RUNNING_ROOT / task.key).unlink(missing_ok=True)
                    futures = {
                        client.submit(
                            _run_spec, self.remakefile, rule.name, task.kwargs, run_seq,
                            pure=False,
                        ): task
                        for task in pending
                    }
                    pending = []
                    # Barrier: drain this rule before starting the next.
                    for future in as_completed(list(futures)):
                        task = futures.pop(future)
                        try:
                            ok = future.result()
                        except Exception as e:
                            marker = RUNNING_ROOT / task.key
                            if not marker.exists() and attempts < 3:
                                # Failed without ever starting (queued on a
                                # worker that died): run it again.
                                future.release()
                                pending.append(task)
                                continue
                            # The worker died running it (KilledWorker) or
                            # the cluster failed it: no sidecar was written,
                            # so record the failure here.
                            marker.unlink(missing_ok=True)
                            ok = False
                            self.rmk.metadata.update_task(
                                task, TASK_STATUS_FAILED,
                                exception=f'{WORKER_DIED}\n\n{type(e).__name__}: {e}',
                            )
                        # Drop the result: a future still held pins its
                        # result on its worker, and if that worker dies the
                        # scheduler recomputes it — re-running a finished
                        # task (H7).
                        future.release()
                        done += 1
                        if ok:
                            logger.info(f'{done}/{ntasks}: {task}')
                        else:
                            record_failure(failures, task)
                            nfailed += 1
                            logger.error(f'{done}/{ntasks} failed: {task}')
        except BaseException:
            # Ctrl-C / SIGTERM: stop the rest of this rule's tasks too (an
            # external scheduler would otherwise keep running them).
            if futures:
                client.cancel(list(futures))
            logger.error(f'interrupted after {done}/{ntasks} task(s)')
            raise
        finally:
            client.close()
            if cluster is not None:
                cluster.close()
        if nfailed:
            skipped = f' ({nskipped} downstream task(s) skipped)' if nskipped else ''
            logger.error(f'{nfailed}/{ntasks} tasks failed{skipped}')
        # Workers wrote sidecars; fold them in so statuses are current.
        self.rmk.metadata.ingest_sidecars(self.rmk.rules)
        return nfailed
