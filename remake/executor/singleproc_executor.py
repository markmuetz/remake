class SingleprocExecutor:
    handles_dependencies = False

    def __init__(self):
        self.completed_task = None

    def can_accept_task(self):
        return self.completed_task is None

    def enqueue_task(self, task):
        task.run(force=True)
        self.completed_task = task

    def get_completed_task(self):
        completed_task = self.completed_task
        self.completed_task = None
        return completed_task

    def finish(self):
        pass