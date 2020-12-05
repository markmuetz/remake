import traceback

from remake.task_control import TaskControl
from remake.task_query_set import TaskQuerySet


class Remake:
    task_ctrl = None
    rules = None
    tasks = None

    @classmethod
    def init(cls, filename=None):
        if not filename:
            stack = next(traceback.walk_stack(None))
            frame = stack[0]
            filename = frame.f_globals['__file__']

        cls.task_ctrl = TaskControl(filename)
        cls.rules = []
        cls.tasks = TaskQuerySet(task_ctrl=cls.task_ctrl)

    @classmethod
    def finalize(cls):
        cls.task_ctrl.finalize()
