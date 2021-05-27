from multiprocessing import Process, Queue, current_process
import logging
import logging.handlers
from time import sleep

from logging import getLogger

from remake.setup_logging import setup_stdout_logging, add_file_logging, remove_file_logging
from remake.load_remake import load_remake
from remake.task import RescanFileTask
from remake.executor.base_executor import Executor

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


def worker(proc_id, task_ctrl_name, task_queue, task_complete_queue, log_queue):
    # sender_log_configurer(log_queue)
    remake = load_remake(task_ctrl_name)
    task_ctrl = remake.task_ctrl
    logger = getLogger(__name__ + '.worker')
    add_file_logging(f'.remake/worker.{proc_id}.log', 'DEBUG')
    # logger.debug('starting')
    task = None
    while True:
        try:
            item = task_queue.get()
            if item is None:
                break
            task_type, task_key, force = item
            # logger.debug(f'{task_type}: {task_key} ({force=})')
            if task_type == 'rescan':
                task = task_ctrl.gen_rescan_task(task_key)
            else:
                task = task_ctrl.task_from_path_hash_key[task_key]
            # logger.debug(f'worker {current_process().name} running {task.path_hash_key()}')
            task.run(force)
            # logger.debug(f'worker {current_process().name} complete {task.path_hash_key()}')
            task_complete_queue.put((task_key, True, None))
        except Exception as e:
            logger.error(e)
            if task:
                logger.error(str(task))
            task_complete_queue.put((task_key, False, e))

            item = task_queue.get()
            if item is None:
                break
    remove_file_logging(f'.remake/worker.{proc_id}.log')

    # logger.debug('stopping')


class MultiprocExecutor(Executor):
    handles_dependencies = False

    def __init__(self, task_ctrl, nproc=8):
        super().__init__(task_ctrl)
        self.nproc = nproc
        self.procs = []

        self.pending_tasks = []
        self.running_tasks = {}
        self.task_queue = None
        self.task_complete_queue = None
        self.log_queue = None
        self.listener = None

    def __enter__(self):
        super().__enter__()

        logger.debug('initializing queues')
        self.task_queue = Queue()
        self.task_complete_queue = Queue()
        self.log_queue = Queue()
        # self.listener = Process(target=log_listener, args=(self.log_queue,))
        # self.listener.start()

        logger.debug(f'creating {self.nproc} workers')
        for i in range(self.nproc):
            proc = Process(target=worker, args=(i,
                                                self.task_ctrl.name,
                                                self.task_queue,
                                                self.task_complete_queue,
                                                self.log_queue))
            proc.daemon = True
            logger.debug(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

        # This causes a hang at e.g. the end of nosetests.
        # sender_log_configurer(self.log_queue)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        logger.debug('finalizing all workers')
        for proc in self.procs:
            self.task_queue.put_nowait(None)

        for proc in self.procs:
            proc.join()

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
        logger.debug(f'adding task {key}: {task}')
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
        remote_task_key, success, error = self.task_complete_queue.get()
        if not success:
            logger.error(f'Error running {remote_task_key}')
            raise Exception(error)
        logger.debug(f'ctrl receieved: {remote_task_key}')
        task_type, completed_task, key = self.running_tasks.pop(remote_task_key)
        logger.debug(f'completed: {completed_task}')
        assert self.can_accept_task()

        if self.pending_tasks:
            self._run_task(self.pending_tasks.pop(0))

        return completed_task

    def has_finished(self):
        return (not self.pending_tasks) and (not self.running_tasks)

