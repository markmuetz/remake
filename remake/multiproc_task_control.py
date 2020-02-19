from multiprocessing import Process, Queue
from remake.task_control import TaskControl


def worker(task_queue, task_complete_queue):
    while True:
        task = task_queue.get()
        print(f'  worker running {task.hexdigest()}')
        task.run()
        task_complete_queue.put(task)


class MultiProcTaskControl(TaskControl):
    def __init__(self, enable_file_task_content_checks=False, nproc=2):
        super().__init__(enable_file_task_content_checks)
        self.nproc = nproc
        self.procs = []

    def run(self, force=False):
        assert self.finalized

        task_queue = Queue()
        task_complete_queue = Queue()

        for i in range(self.nproc):
            proc = Process(target=worker, args=(task_queue, task_complete_queue))
            proc.start()
            self.procs.append(proc)

        running_tasks = {}

        # import ipdb; ipdb.set_trace()
        while self.pending_tasks or self.remaining_tasks or self.running_tasks:
            try:
                task = next(self.get_next_pending())
            except StopIteration:
                task = None

            if not task:
                print('ctrl no tasks available - wait for completed')
                remote_completed_task = task_complete_queue.get()
                print(f'ctrl receieved: {remote_completed_task.hexdigest()} {remote_completed_task}')
                completed_task = running_tasks.pop(remote_completed_task.hexdigest())
                self.task_complete(completed_task)
            else:
                task_run_index = len(self.completed_tasks) + len(self.running_tasks)
                print(f'ctrl running {task_run_index}/{len(self.tasks)}: {task.hexdigest()} {task}')
                running_tasks[task.hexdigest()] = task
                task_queue.put(task)

        for proc in self.procs:
            proc.terminate()
