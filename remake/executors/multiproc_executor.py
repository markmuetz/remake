import sys
import math
from io import StringIO
from pathlib import Path
from multiprocessing import Process, Queue, current_process, cpu_count

from loguru import logger

from ..loader import load_remake
from .executor import Executor


class Capturing(list):
    """Capture stdout from function.

    https://stackoverflow.com/a/16571630/54557
    """
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout


def worker(proc_id, remakefile_name, task_queue, task_complete_queue):
    logger.remove()
    logfile = Path(f'.remake/{remakefile_name}/worker.{proc_id}.log')
    logfile.parent.mkdir(exist_ok=True, parents=True)
    logger.add(logfile, level='DEBUG')

    rmk = load_remake(remakefile_name, finalize=False)
    logger.debug('starting')
    task = None
    while True:
        try:
            item = task_queue.get()
            if item is None:
                break
            task_key, force = item
            logger.debug(f'worker {current_process().name} running {task}')
            with Capturing() as output:
                rmk.run_tasks_from_keys([task_key], 'SingleprocExecutor')
            logger.debug(f'worker {current_process().name} stdout')
            for line in output:
                logger.debug(line)
            # task.run(use_task_control=False)
            logger.debug(f'worker {current_process().name} complete {task}')
            task_complete_queue.put((task_key, True, None))
        except Exception as e:
            logger.error(e)
            if task:
                logger.error(str(task))
            task_complete_queue.put((task_key, False, e))

            item = task_queue.get()
            if item is None:
                break
    logger.debug('stopping')


class MultiprocExecutor(Executor):
    def __init__(self, rmk, multiproc_config=None):
        if not multiproc_config:
            multiproc_config = {'nproc': cpu_count()}
        super().__init__(rmk)

        self.nproc = multiproc_config['nproc']
        logger.info(f'Using {self.nproc} processes')
        self.procs = []

        self.running_tasks = {}
        self.task_queue = None
        self.task_complete_queue = None
        self.already_run_tasks = set()
        self.all_tasks = set()

    def _init_queues_procs(self):
        logger.trace('initializing queues')
        self.task_queue = Queue()
        self.task_complete_queue = Queue()

        logger.trace(f'creating {self.nproc} workers')
        for i in range(self.nproc):
            proc = Process(target=worker, args=(i, self.rmk.name,
                                                self.task_queue,
                                                self.task_complete_queue))
            proc.daemon = True
            logger.trace(f'created proc {proc}')
            proc.start()
            self.procs.append(proc)

    def _finalize_queues_procs(self):
        logger.trace('finalizing all workers')
        for proc in self.procs:
            self.task_queue.put_nowait(None)

        for proc in self.procs:
            proc.join()

    def _can_run_task(self, task):
        can_run = True
        for prev_task in task.prev_tasks:
            if prev_task not in self.all_tasks:
                continue
            if prev_task not in self.already_run_tasks:
                can_run = False
                break
        return can_run

    def _enqueue_task(self, task):
        key = task.key()
        logger.trace(f'enqueuing task {key}: {task}')
        self.running_tasks[key] = (task, key)
        self.task_queue.put((key, True))

    def _wait_for_complete(self):
        logger.trace('ctrl no tasks available - wait for completed')
        remote_task_key, success, error = self.task_complete_queue.get()
        if not success:
            logger.error(f'Error running {remote_task_key}')
            raise Exception(error)
        logger.trace(f'ctrl receieved: {remote_task_key}')
        completed_task, key = self.running_tasks.pop(remote_task_key)
        completed_task.requires_rerun = False
        self.already_run_tasks.add(completed_task)
        logger.trace(f'completed: {completed_task}')
        return completed_task

    def run_tasks(self, rerun_tasks):
        ntasks = len(rerun_tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1
        ntasks_run = 0

        tasks_to_run = [*rerun_tasks]
        self.all_tasks.update(tasks_to_run)
        try:
            self._init_queues_procs()
            while len(self.already_run_tasks) < len(rerun_tasks):
                if tasks_to_run:
                    task = tasks_to_run[0]
                    can_run = self._can_run_task(task)
                    logger.debug(f'Can run: {can_run} {task}')
                else:
                    can_run = False

                if can_run:
                    tasks_to_run.pop(0)
                    logger.info(f'{ntasks_run + 1:>{ndigits}}/{ntasks}: {task} enqueued')
                    self._enqueue_task(task)
                    ntasks_run += 1
                else:
                    completed_task = self._wait_for_complete()
                    logger.info(f'{" ":>{2 * ndigits + 1}}: {completed_task} completed')
        finally:
            self._finalize_queues_procs()
