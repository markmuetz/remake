import abc

from ..util.code_compare import CodeComparer


class MetadataManager(abc.ABC):
    def __init__(self):
        self.code_comparer = CodeComparer()

    @abc.abstractmethod
    def get_or_create_rule_metadata(self, rule):
        pass

    @abc.abstractmethod
    def get_or_create_tasks_metadata(self, tasks):
        pass

    @abc.abstractmethod
    def update_task_metadata(self, task, exception=''):
        pass
