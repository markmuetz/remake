from multiprocessing import Process, Queue, current_process
from remake.task_control import TaskControl, compare_task_with_previous_runs, task_requires_rerun_based_on_contents
import logging
import logging.handlers
from logging import getLogger

logger = getLogger(__name__)


def listener_configurer():
    root = logging.getLogger()
    h = logging.StreamHandler()
    f = logging.Formatter('%(asctime)s %(processName)-15s %(name)-40s %(levelname)-8s %(message)s')
    root.setLevel(logging.INFO)
    h.setFormatter(f)
    root.addHandler(h)


def log_listener(log_queue):
    listener_configurer()
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


def worker(task_queue, task_complete_queue, log_queue):
    sender_log_configurer(log_queue)
    logger = getLogger(__name__ + '.worker')
    logger.debug('starting')
    while True:
        item = task_queue.get()
        if item is None:
            logger.debug('stopping')
            break
        task, force = item
        logger.debug(f'worker {current_process().name} running {task.hexdigest()}')
        task.run(force)
        logger.debug(f'worker {current_process().name} complete {task.hexdigest()}')
        task_complete_queue.put(task)


class MultiProcTaskControl(TaskControl):
    def __init__(self, enable_file_task_content_checks=False, nproc=2):
        super().__init__(enable_file_task_content_checks)
        self.nproc = nproc
        self.procs = []

    def run(self, force=False):
        assert self.finalized
        if not (self.pending_tasks or self.remaining_tasks or self.running_tasks):
            return

        task_queue = Queue()
        task_complete_queue = Queue()
        log_queue = Queue()

        listener = Process(target=log_listener, args=(log_queue,))
        listener.start()

        for i in range(self.nproc):
            proc = Process(target=worker, args=(task_queue, task_complete_queue, log_queue))
            logger.debug(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

        sender_log_configurer(log_queue)
        running_tasks = {}

        # import ipdb; ipdb.set_trace()
        while self.pending_tasks or self.remaining_tasks or self.running_tasks:
            try:
                task = next(self.get_next_pending())
                logger.debug(f'ctrl got task {task}')
            except StopIteration:
                task = None

            if not task:
                logger.debug('ctrl no tasks available - wait for completed')
                remote_completed_task = task_complete_queue.get()
                logger.debug(f'ctrl receieved: {remote_completed_task.hexdigest()} {remote_completed_task}')
                completed_task, task_sha1hex = running_tasks.pop(remote_completed_task.hexdigest())

                if self.enable_file_task_content_checks:
                    logger.debug('performing task file contents checks and writing data')
                    task_requires_rerun_based_on_contents(self.file_metadata_dir, completed_task, task_sha1hex, True)
                    if self.extra_checks:
                        requires_rerun = task_requires_rerun_based_on_contents(self.file_metadata_dir, completed_task,
                                                                               task_sha1hex)
                        assert not requires_rerun
                self.task_complete(completed_task)
            else:
                task_run_index = len(self.completed_tasks) + len(self.running_tasks)
                logger.debug(f'ctrl submitting {task_run_index}/{len(self.tasks)}: {task.hexdigest()} {task}')
                print(f'{task_run_index}/{len(self.tasks)}: {task}')
                task_sha1hex = None
                if self.enable_file_task_content_checks:
                    logger.debug('performing task file contents checks')
                    requires_rerun, task_sha1hex = compare_task_with_previous_runs(self.file_metadata_dir,
                                                                                   self.task_metadata_dir,
                                                                                   task, overwrite_task_metadata=False)
                    force = force or requires_rerun
                running_tasks[task.hexdigest()] = (task, task_sha1hex)
                task_queue.put((task, force))

        logger.debug('all tasks complete')
        logger.debug('terminating all procs')

        for proc in self.procs:
            task_queue.put_nowait(None)

        for proc in self.procs:
            proc.terminate()

        log_queue.put_nowait(None)
        # listener.terminate()
        listener.join(5)
        listener.terminate()

    def run_one(self, force=False):
        raise Exception(f'Not implmented for subclass {type(self)}')

