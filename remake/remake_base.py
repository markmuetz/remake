from logging import getLogger
from pathlib import Path
import traceback

from remake.task_control import TaskControl
from remake.task_query_set import TaskQuerySet
from remake.setup_logging import setup_stdout_logging

logger = getLogger(__name__)


class Remake:
    remakes = {}
    current_remake = None

    def __init__(self, name=None):
        setup_stdout_logging('INFO', colour=True)
        if not name:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            name = frame.f_globals['__file__']
        if name in Remake.remakes:
            # Can happen on ipython run remakefile.
            logger.info(f'Remake {name} added twice')
        Remake.remakes[name] = self
        Remake.current_remake = self

        self.config = None
        self.task_ctrl = TaskControl(name)
        self.rules = []
        self.tasks = TaskQuerySet(task_ctrl=self.task_ctrl)

    def run_all(self):
        self.task_ctrl.run()

    def run_one(self):
        self.task_ctrl.run_one()

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

    def finalize(self):
        self.task_ctrl.finalize()
