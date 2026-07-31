"""Sidecar result files.

How per-task SLURM array processes record results without touching the
shared SQLite DB: concurrent writers livelock SQLite on NFS-class
filesystems past a few hundred processes (see
design_docs/slurm_implementation.md, "Sidecar result files", and
tests/benchmarks/bench_sqlite_contention.py). `remake run-array-task`
writes one JSON sidecar per result; the next invocation that reads the
DB (plan/run/info) ingests pending sidecars in a single batched
transaction (`Sqlite3Backend.ingest_sidecars`) and deletes them.

Sharded like the per-task logs (design_docs/per_task_logging.md), but
self-cleaning: sidecars are deleted on ingest.
"""
import json
import time
from pathlib import Path

from ..core.scope import io_hash as compute_io_hash
from ..core.scope import uses_hash as compute_uses_hash
from .metadata_manager import MetadataManager

RESULTS_ROOT = Path('.remake/tasks/results')


def task_result_path(rule_name, key):
    return RESULTS_ROOT / rule_name / key[:2] / f'{key[2:]}.json'


class SidecarWriter(MetadataManager):
    """Metadata backend for per-task array processes: records results as
    sidecar files and opens no DB connection at all."""

    def __init__(self, run_seq=None):
        # Assigned on the submit node and read from the job spec, so every
        # array element of one submission records the same run_seq regardless
        # of which compute node ran it (no node-clock dependence).
        self.run_seq = run_seq

    def ensure_rules(self, rules, remakefile=None):
        pass

    def get_tasks_status(self, tasks):
        return {}

    def current_run_seq(self):
        return self.run_seq

    def update_task(self, task, status, exception='', resources=None):
        path = task_result_path(task.rule.name, task.key)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            'status': status,
            'exception': exception,
            # Measured on the compute node; written to the DB at ingest.
            # Absent in pre-0.9 sidecars, which ingest as NULLs.
            'resources': resources or {},
            'uses_hash': compute_uses_hash(task.rule.uses),
            'io_hash': compute_io_hash(task.rule),
            # Run source as it exists on the compute node: ingest must record
            # what actually ran, not what the ingesting process has on disk
            # (design_docs/bugs/05_slurm_sidecar_run_code_not_recorded.md).
            'run_hash': task.rule.source['run'],
            'run_seq': self.run_seq,
            # Matches the format sqlite's datetime('now') stores (UTC).
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
        }
        # Write-then-rename so ingestion never reads a torn write.
        tmp = path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(payload))
        tmp.rename(path)
