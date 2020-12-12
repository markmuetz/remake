from logging import getLogger
import multiprocessing
from pathlib import Path
import traceback

from remake.task_control import TaskControl
from remake.task_query_set import TaskQuerySet
from remake.setup_logging import setup_stdout_logging

logger = getLogger(__name__)


class Remake:
    remakes = {}
    current_remake = {}

    def __init__(self, name=None):
        setup_stdout_logging('INFO', colour=True)
        if not name:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            name = frame.f_globals['__file__']
        if multiprocessing.current_process().name == 'MainProcess':
            if name in Remake.remakes:
                # Can happen on ipython run remakefile.
                logger.info(f'Remake {name} added twice')
            Remake.remakes[name] = self
        else:
            logger.info(f'Process {multiprocessing.current_process().name}')
            logger.info(Remake.current_remake)
            logger.info(Remake.remakes)

        Remake.current_remake[multiprocessing.current_process().name] = self

        self.config = None
        self.task_ctrl = TaskControl(name)
        self.rules = []
        self.tasks = TaskQuerySet(task_ctrl=self.task_ctrl)

    def run_all(self, force=False):
        self.task_ctrl.run_all(force=force)

    def run_one(self, force=False):
        try:
            task = next(self.task_ctrl.get_next_pending())
            self.run_requested([task], force=force)
        except StopIteration:
            pass

    def run_requested(self, requested, force=False):
        self.task_ctrl.run_requested(requested, force=force)

    def file_info(self, filepath):
        filepath = Path(filepath).absolute()
        if filepath in self.task_ctrl.input_task_map:
            used_by_tasks = self.task_ctrl.input_task_map[filepath]
        else:
            used_by_tasks = []
        if filepath in self.task_ctrl.output_task_map:
            produced_by_task = self.task_ctrl.output_task_map[filepath]
        else:
            produced_by_task = None
        if used_by_tasks or produced_by_task:
            path_md = self.task_ctrl.metadata_manager.path_metadata_map[filepath]
        else:
            path_md = None
        return path_md, used_by_tasks, produced_by_task

    @property
    def finalized(self):
        return self.task_ctrl.finalized

    def reset(self):
        self.task_ctrl.reset()
        return self

    def finalize(self):
        self.task_ctrl.finalize()
        return self
