import abc


class Executor(abc.ABC):
    def __init__(self, rmk):
        self.rmk = rmk

    @abc.abstractmethod
    def run_tasks(self, tasks):
        pass
