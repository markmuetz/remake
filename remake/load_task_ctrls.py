from pathlib import Path
import inspect

# from remake import Remake
from remake.util import load_module


def load_remake(filename):
    # Avoids circular import.
    from remake import Remake
    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    remake_module = load_module(filename)
    # remakes = [o for o in [getattr(remake_module, m) for m in dir(remake_module)]
    #            if o.__class__.__name__ == 'Remake']
    remakes = [o for o in [getattr(remake_module, m) for m in dir(remake_module)]
               if isinstance(o, Remake)]
    if len(remakes) > 1:
        raise Exception(f'More than one remake defined in {filename}')
    elif not remakes:
        raise Exception(f'No remake defined in {filename}')
    return remakes[0]


def load_task_ctrls(filename):
    if not Path(filename).suffix:
        filename = Path(filename).with_suffix('.py')
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
        remakefiles = [o for o in [getattr(task_ctrl_module, m) for m in dir(task_ctrl_module)]
                       if o.__class__.__name__ == 'Remake']
        for remakefile in remakefiles:
            task_ctrls.append(remakefile.task_ctrl)

    if not task_ctrls:
        raise Exception(f'No task controls defined in {filename}')

    return task_ctrls
