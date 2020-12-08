import inspect

# from remake import Remake
from remake.util import load_module


def load_task_ctrls(filename):
    task_ctrl_module = load_module(filename)
    task_ctrls = []
    functions = [o for o in [getattr(task_ctrl_module, m) for m in dir(task_ctrl_module)]
                 if inspect.isfunction(o)]
    for func in functions:
        if hasattr(func, 'is_remake_task_control') and func.is_remake_task_control:
            task_ctrl = func()
            # if not isinstance(task_ctrl, TaskControl):
            #     raise Exception(f'{task_ctrl} is not a TaskControl (defined in {func})')
            task_ctrls.append(task_ctrl)
    if not task_ctrls:
        classes = [o for o in [getattr(task_ctrl_module, m) for m in dir(task_ctrl_module)]
                   if inspect.isclass(o)]
        for cls in classes:
            if cls.__name__ == 'Remake':
                task_ctrls.append(cls.task_ctrl)

    if not task_ctrls:
        raise Exception(f'No task controls defined in {filename}')

    return task_ctrls
