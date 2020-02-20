from multiprocessing import Process, Queue, current_process
import logging
import logging.handlers
from logging import getLogger

from remake.task_control import TaskControl
from remake.setup_logging import setup_stream_logging

logger = getLogger(__name__)


def log_listener(log_queue):
    setup_stream_logging(logging.INFO)
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
            logger.debug(f'worker {current_process().name} running {task.hexdigest()}')
            task.run(force)
            logger.debug(f'worker {current_process().name} complete {task.hexdigest()}')
            task_complete_queue.put(task)
        except Exception as e:
            logger.error(e)
            logger.error(str(task))
            error_queue.put(e)
            break

    logger.debug('stopping')


class MultiProcTaskControl(TaskControl):
    def __init__(self, enable_file_task_content_checks=False, nproc=2):
        super().__init__(enable_file_task_content_checks)
        self.nproc = nproc
        self.procs = []

    def run(self, force=False, display_func=None):
        assert self.finalized
        if not (self.pending_tasks or self.remaining_tasks or self.running_tasks):
            return

        task_queue = Queue()
        task_complete_queue = Queue()
        error_queue = Queue()
        log_queue = Queue()

        listener = Process(target=log_listener, args=(log_queue,))
        listener.start()

        for i in range(self.nproc):
            proc = Process(target=worker, args=(task_queue, task_complete_queue, error_queue, log_queue))
            logger.debug(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

        sender_log_configurer(log_queue)
        running_tasks = {}

        try:
            # import ipdb; ipdb.set_trace()
            while self.pending_tasks or self.remaining_tasks or self.running_tasks:
                if len(self.running_tasks) < self.nproc:
                    # N.B. get_next_pending totally happy to hand out as many tasks as are pending.
                    # The only reason for this check is so that the display of running tasks matches expectations;
                    # there will only be nproc running tasks at any time.
                    try:
                        task = next(self.get_next_pending())
                        logger.debug(f'ctrl got task {task}')
                    except StopIteration:
                        task = None
                else:
                    task = None

                if not error_queue.empty():
                    error = error_queue.get()
                    raise error

                if not task:
                    logger.debug('ctrl no tasks available - wait for completed')
                    remote_completed_task = task_complete_queue.get()
                    logger.debug(f'ctrl receieved: {remote_completed_task.hexdigest()} {remote_completed_task}')
                    completed_task, task_sha1hex = running_tasks.pop(remote_completed_task.hexdigest())

                    if self.enable_file_task_content_checks:
                        task_md = self.task_metadata_map[completed_task]
                        self._post_run_with_content_check(task_md)
                    self.task_complete(completed_task)
                    if display_func:
                        display_func(self)
                else:
                    task_run_index = len(self.completed_tasks) + len(self.running_tasks)
                    logger.debug(f'ctrl submitting {task_run_index}/{len(self.tasks)}: {task.hexdigest()} {task}')
                    print(f'{task_run_index}/{len(self.tasks)}: {task}')
                    task_sha1hex = None
                    if self.enable_file_task_content_checks:
                        task_md = self.task_metadata_map[task]
                        requires_rerun = self._task_requires_run_with_content_check(task_md)
                        requires_rerun = force or requires_rerun
                        if requires_rerun:
                            logger.debug(f'running task (force={requires_rerun}) {task}')
                            running_tasks[task.hexdigest()] = (task, task_sha1hex)
                            task_queue.put((task, True))
                            if display_func:
                                display_func(self)
                        else:
                            logger.debug(f'no longer requires rerun: {task}')
                            self.task_complete(task)
                    else:
                        running_tasks[task.hexdigest()] = (task, task_sha1hex)
                        task_queue.put((task, force))
                        if display_func:
                            display_func(self)

            logger.debug('all tasks complete')
            logger.debug('terminating all procs')
        except Exception as e:
            logger.exception(e)
            raise
        finally:
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

