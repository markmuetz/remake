import abc
from logging import getLogger

logger = getLogger(__name__)


class Executor(abc.ABC):
    handles_dependencies = False

    def __init__(self, task_ctrl):
        self.task_ctrl = task_ctrl

    def __enter__(self):
        logger.debug('entering executor body')

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug('exiting')
        if exc_type or exc_val or exc_tb:
            logger.error('There were problems!')
            logger.error(exc_type)
            logger.error(exc_val)
            logger.error(exc_tb)

    @abc.abstractmethod
    def can_accept_task(self):
        pass

    @abc.abstractmethod
    def enqueue_task(self, task):
        pass

    @abc.abstractmethod
    def get_completed_task(self):
        pass

    @abc.abstractmethod
    def has_finished(self):
        pass
