from remake.executor.base_executor import Executor


class SingleprocExecutor(Executor):
    def __init__(self, task_ctrl):
        super().__init__(task_ctrl)
        self.completed_task = None

    def can_accept_task(self):
        return self.completed_task is None

    def enqueue_task(self, task):
        task.run(force=True)
        self.completed_task = task

    def get_completed_task(self):
        completed_task = self.completed_task
        assert completed_task is not None, 'completed_task is None'
        self.completed_task = None
        return completed_task

    def has_finished(self):
        return not self.completed_task

