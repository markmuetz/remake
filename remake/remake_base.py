from pathlib import Path
import traceback

from remake.task_control import TaskControl
from remake.task_query_set import TaskQuerySet
from remake.setup_logging import setup_stdout_logging


class Remake:
    config = None
    task_ctrl = None
    rules = None
    tasks = None

    @classmethod
    def init(cls, filename=None):
        setup_stdout_logging('INFO', colour=True)
        if not filename:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            filename = frame.f_globals['__file__']

        cls.task_ctrl = TaskControl(filename)
        cls.rules = []
        cls.tasks = TaskQuerySet(task_ctrl=cls.task_ctrl)

    @classmethod
    def file_info(cls, filepath):
        filepath = Path(filepath).absolute()
        if filepath in cls.task_ctrl.input_task_map:
            used_by_tasks = cls.task_ctrl.input_task_map[filepath]
        else:
            used_by_tasks = []
        if filepath in cls.task_ctrl.output_task_map:
            produced_by_task = cls.task_ctrl.output_task_map[filepath]
        else:
            produced_by_task = None
        if used_by_tasks or produced_by_task:
            path_md = cls.task_ctrl.metadata_manager.path_metadata_map[filepath]
        else:
            path_md = None
        return path_md, used_by_tasks, produced_by_task

    @classmethod
    def finalize(cls):
        cls.task_ctrl.finalize()
