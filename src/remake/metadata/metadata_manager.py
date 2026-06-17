import abc
from dataclasses import dataclass
from typing import Optional

TASK_STATUS_PENDING = 0
TASK_STATUS_SUCCESS = 1
TASK_STATUS_FAILED = 2


@dataclass(frozen=True)
class TaskRecord:
    """A task's stored execution state. The planner consumes these; Task
    objects themselves stay pure value objects."""

    key: str
    status: int
    timestamp: Optional[str]
    run_code: str
    uses_hash: str
    exception: str
    io_hash: Optional[str] = None


class MetadataManager(abc.ABC):
    @abc.abstractmethod
    def ensure_rules(self, rules):
        """Create/update stored rule metadata (source code) for these rules."""

    @abc.abstractmethod
    def get_tasks_status(self, tasks) -> dict:
        """{task.key: TaskRecord} for tasks that have a stored record."""

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
