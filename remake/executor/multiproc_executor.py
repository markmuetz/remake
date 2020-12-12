from multiprocessing import Process, Queue, current_process
import logging
import logging.handlers

from logging import getLogger

from remake.setup_logging import setup_stdout_logging
from remake.load_task_ctrls import load_task_ctrls
from remake.task import RescanFileTask

logger = getLogger(__name__)


def log_listener(log_queue):
    # setup_stdout_logging(logging.INFO)
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
            import sys
            import traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def sender_log_configurer(log_queue):
    h = logging.handlers.QueueHandler(log_queue)  # Just the one handler needed
    remake_root = logging.getLogger('remake')
    remake_root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    # remake_root.setLevel(logging.INFO)


def worker(task_ctrl_name, task_queue, task_complete_queue, error_queue, log_queue):
    # sender_log_configurer(log_queue)
    task_ctrl = load_task_ctrls(task_ctrl_name + '.py')[0]
    logger = getLogger(__name__ + '.worker')
    logger.debug('starting')
    task = None
    while True:
        try:
            item = task_queue.get()
            if item is None:
                break
            task_type, task_key, force = item
            logger.debug(f'{task_type}: {task_key} ({force=})')
            if task_type == 'rescan':
                task = task_ctrl.gen_rescan_task(task_key)
            else:
                task = task_ctrl.task_from_path_hash_key[task_key]
            logger.debug(f'worker {current_process().name} running {task.path_hash_key()}')
            task.run(force)
            logger.debug(f'worker {current_process().name} complete {task.path_hash_key()}')
            task_complete_queue.put(task_key)
        except Exception as e:
            logger.error(e)
            if task:
                logger.error(str(task))
            error_queue.put(e)

            item = task_queue.get()
            if item is None:
                break

    logger.debug('stopping')


class MultiprocExecutor:
    handles_dependencies = False

    def __init__(self, task_ctrl, nproc=8):
        self.task_ctrl = task_ctrl
        self.nproc = nproc
        self.procs = []

        self.pending_tasks = []
        self.running_tasks = {}
        self.task_queue = None
        self.task_complete_queue = None
        self.error_queue = None
        self.log_queue = None
        self.listener = None

    def init(self):
        logger.debug('initializing')
        self.task_queue = Queue()
        self.task_complete_queue = Queue()
        self.error_queue = Queue()
        self.log_queue = Queue()
        self.listener = Process(target=log_listener, args=(self.log_queue,))
        self.listener.start()

        logger.debug(f'Creating {self.nproc} workers')
        for i in range(self.nproc):
            proc = Process(target=worker, args=(self.task_ctrl.name,
                                                self.task_queue,
                                                self.task_complete_queue,
                                                self.error_queue,
                                                self.log_queue))
            logger.debug(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

        sender_log_configurer(self.log_queue)

    def _run_task(self, task):
        # N.B. Cannot send Tasks that are build from rules as they do not pickle.
        # Perhaps because of metaclass?
        # Send a key and extract task from a task_ctrl on other side.
        if isinstance(task, RescanFileTask):
            task_type = 'rescan'
            key = str(task.inputs['filepath'])
        else:
            task_type = 'task'
            key = task.path_hash_key()
        self.running_tasks[key] = (task_type, task, key)
        self.task_queue.put((task_type, key, True))

    def can_accept_task(self):
        return len(self.running_tasks) < self.nproc

    def enqueue_task(self, task):
        if self.can_accept_task():
            self._run_task(task)
        else:
            self.pending_tasks.append(task)

    def get_completed_task(self):
        logger.debug('ctrl no tasks available - wait for completed')
        remaote_task_key = self.task_complete_queue.get()
        logger.debug(f'ctrl receieved: {remaote_task_key}')
        task_type, completed_task, key = self.running_tasks.pop(remaote_task_key)
        logger.debug(f'completed: {completed_task}')
        assert self.can_accept_task()

        if self.pending_tasks:
            self._run_task(self.pending_tasks.pop(0))

        return completed_task

    def has_finished(self):
        return (not self.pending_tasks) and (not self.running_tasks)

    def finish(self):
        logger.debug('finalizing all workers')
        for proc in self.procs:
            self.task_queue.put_nowait(None)

        for proc in self.procs:
            proc.terminate()

        self.log_queue.put_nowait(None)
        # listener.terminate()
        # self.listener.join(5)
        self.listener.terminate()
