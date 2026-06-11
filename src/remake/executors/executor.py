import abc


class Executor(abc.ABC):
    def __init__(self, rmk):
        self.rmk = rmk

    @abc.abstractmethod
    def run_tasks(
        self, rerun_tasks, show_reasons=False, show_task_code_diff=False, stdout_to_log=False
    ):
        pass
