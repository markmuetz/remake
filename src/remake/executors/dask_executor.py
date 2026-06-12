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
from ..core.planner import upstream_failed
from .executor import Executor

_worker_rmk_cache = {}


def _run_spec(remakefile, rule_name, kwargs):
    """Runs on a dask worker. Returns True on success."""
    from ..loader import load_remake
    from ..metadata.sidecar import SidecarWriter
    from ..util import task_log_path

    rmk = _worker_rmk_cache.get(remakefile)
    if rmk is None:
        rmk = load_remake(remakefile, finalize=False)
        rmk.metadata = SidecarWriter()
        _worker_rmk_cache[remakefile] = rmk
    task = rmk.task_from_spec(rule_name, kwargs)
    logfile = task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    sink_id = logger.add(logfile, level='DEBUG', mode='w')
    try:
        rmk.run_task(task)
        return True
    except Exception:
        return False  # recorded (sidecar + log) by run_task
    finally:
        logger.remove(sink_id)


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
        failures = {}  # rule -> set of frozenset(kwargs.items())
        client, cluster = self._client()
        try:
            for rule, rule_tasks in groups:
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
                logger.info(f'{rule.name}: {len(to_run)} task(s) on dask ({self.nproc} workers)')
                futures = {
                    client.submit(
                        _run_spec, self.remakefile, rule.name, task.kwargs, pure=False
                    ): task
                    for task in to_run
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
