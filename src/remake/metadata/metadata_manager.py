import abc
from dataclasses import dataclass
from typing import Optional

TASK_STATUS_PENDING = 0
TASK_STATUS_SUCCESS = 1
TASK_STATUS_FAILED = 2

# Human-readable names for the stored status codes; None (no record) and an
# unmapped code both read as 'pending' via STATUS_NAMES.get(status, 'pending').
STATUS_NAMES = {
    None: 'pending',
    TASK_STATUS_SUCCESS: 'success',
    TASK_STATUS_FAILED: 'failed',
}


@dataclass(frozen=True)
class TaskRecord:
    """A task's stored execution state. The planner consumes these; Task
    objects themselves stay pure value objects.

    Code/uses/io are carried as integer ids into the content-addressed `code`
    table, not as text — fetching the text per task is what made status
    queries scale with task count (design_docs/logs_analysis/README.md §1.2).
    Consumers resolve ids to text via `MetadataManager.get_codes`, once per
    distinct id rather than once per task."""

    key: str
    status: int
    timestamp: Optional[str]
    run_code_id: Optional[int]
    uses_code_id: Optional[int]
    exception: str
    io_code_id: Optional[int] = None  # None = pre-upgrade record (not tracked)
    run_seq: Optional[int] = None  # None for pre-upgrade/never-stamped records


class MetadataManager(abc.ABC):
    @abc.abstractmethod
    def ensure_rules(self, rules, remakefile=None):
        """Create/update stored rule metadata (source code) for these rules.
        `remakefile` records which file defined them (provenance + the
        duplicate-rule-name guard)."""

    @abc.abstractmethod
    def get_tasks_status(self, tasks) -> dict:
        """{task.key: TaskRecord} for tasks that have a stored record."""

    def get_codes(self, code_ids) -> dict:
        """{code_id: text} for the given ids (a TaskRecord's
        run_code_id/uses_code_id/io_code_id). Backends without a code store
        have nothing to resolve; None ids are ignored."""
        return {}

    def get_uses_manifest(self, uses_code_id) -> dict:
        """{name: (raw-rendering, kind)} for the helpers behind a stored uses
        version (a TaskRecord's uses_code_id) — see scope.raw_uses_parts for
        the kinds. Display only; empty when unknown (backends without a store,
        or records predating the manifest table)."""
        return {}

    def begin_invocation(self):
        """Start a new logical invocation: allocate a fresh run_seq so tasks
        committed from here share one stamp, distinct from earlier invocations.
        A no-op for backends without a persistent counter."""

    def current_run_seq(self):
        """This invocation's run_seq (a monotonic stamp shared by every task it
        commits). Backends without a persistent counter return None — no
        durable propagation, only in-pass."""
        return None

    @abc.abstractmethod
    def update_task(self, task, status, exception=''):
        """Record a task execution result."""

    def update_tasks(self, tasks, status, exception=''):
        """Record the same state for many tasks (backends may batch)."""
        for task in tasks:
            self.update_task(task, status, exception)

    def delete_tasks(self, tasks):
        """Remove stored records (tasks become never-run/pending)."""
        raise NotImplementedError(f'{type(self).__name__} cannot delete records')

    def ingest_sidecars(self, rules):
        """Absorb pending sidecar result files (written by per-task array
        processes) for these rules. Backends without a persistent store
        have nothing to ingest into; default is a no-op."""
        return 0
