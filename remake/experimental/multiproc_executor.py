from multiprocessing import Process, Queue, current_process
import logging
import logging.handlers
from logging import getLogger

from remake.setup_logging import setup_stdout_logging

logger = getLogger(__name__)


def log_listener(log_queue):
    setup_stdout_logging(logging.INFO)
    listener_logger = getLogger(__name__ + '.listener')
    listener_logger.debug('Starting')
    while True:
        try:
            record = log_queue.get()
            if record is None:
                listener_logger.debug('Ending')
                break
            logger = getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def sender_log_configurer(log_queue):
    h = logging.handlers.QueueHandler(log_queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.INFO)


def worker(task_queue, task_complete_queue, error_queue, log_queue):
    sender_log_configurer(log_queue)
    logger = getLogger(__name__ + '.worker')
    logger.debug('starting')
    while True:
        try:
            item = task_queue.get()
            if item is None:
                break
            task, force = item
            logger.debug(f'worker {current_process().name} running {task.path_hash_key()}')
            task.run(force)
            logger.debug(f'worker {current_process().name} complete {task.path_hash_key()}')
            task_complete_queue.put(task)
        except Exception as e:
            logger.error(e)
            logger.error(str(task))
            error_queue.put(e)
            break

    logger.debug('stopping')


class MultiprocExecutor:
    def __init__(self, nproc=2):
        self.nproc = nproc
        self.procs = []
        self.task_queue = Queue()
        self.task_complete_queue = Queue()
        self.error_queue = Queue()
        self.log_queue = Queue()

        self.listener = Process(target=log_listener, args=(self.log_queue,))
        self.listener.start()

        for i in range(self.nproc):
            proc = Process(target=worker, args=(self.task_queue,
                                                self.task_complete_queue,
                                                self.error_queue,
                                                self.log_queue))
            logger.debug(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

        sender_log_configurer(self.log_queue)

        self.pending_tasks = []
        self.running_tasks = {}

    def _run_task(self, task):
        task_sha1hex = None
        self.running_tasks[task.hexdigest()] = (task, task_sha1hex)
        self.task_queue.put((task, True))

    def can_accept_task(self):
        return len(self.running_tasks) < self.nproc

    def enqueue_task(self, task):
        if self.can_accept_task():
            self._run_task(task)
        else:
            self.pending_tasks.append(task)

    def get_completed_task(self):
        logger.debug('ctrl no tasks available - wait for completed')
        remote_completed_task = self.task_complete_queue.get()
        logger.debug(f'ctrl receieved: {remote_completed_task.path_hash_key()} {remote_completed_task}')
        completed_task, task_sha1hex = self.running_tasks.pop(remote_completed_task.path_hash_key())
        logger.debug(f'completed: {completed_task}')
        assert self.can_accept_task()

        if self.pending_tasks:
            self._run_task(self.pending_tasks.pop(0))

        return completed_task

    def finish(self):
        for proc in self.procs:
            self.task_queue.put_nowait(None)

        for proc in self.procs:
            proc.terminate()

        self.log_queue.put_nowait(None)
        # listener.terminate()
        self.listener.join(5)
        self.listener.terminate()
