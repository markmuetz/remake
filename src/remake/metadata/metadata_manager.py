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
