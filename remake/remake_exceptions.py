class RemakeError(Exception):
    pass


class CyclicDependency(RemakeError):
    def __init__(self, msg, dependent_tasks):
        super().__init__(msg)
        self.dependent_tasks = dependent_tasks
