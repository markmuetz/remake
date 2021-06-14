class RemakeError(Exception):
    pass


class MissingTaskRuleProperty(RemakeError):
    pass


class TaskRuleNameError(RemakeError):
    pass


class RemakeLoadError(RemakeError):
    pass


class FileNotCreated(RemakeError):
    pass


class CyclicDependency(RemakeError):
    def __init__(self, msg, dependent_tasks):
        super().__init__(msg)
        self.dependent_tasks = dependent_tasks
