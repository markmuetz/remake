import inspect
import sys
import importlib.util
import hashlib
from logging import getLogger
from pathlib import Path

from remake import TaskControl
from remake.remake_cmd import logger

logger = getLogger(__name__)

# BUF_SIZE is totally arbitrary, change for your app!
SHA1_BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


def tmp_to_actual_path(path: Path) -> Path:
    if not path.name[:12] == '.remake.tmp.':
        raise ValueError(f'Path must be a remake tmp path (start with ".remake.tmp."): {path}')

    return path.parent / path.name[12:]


def sha1sum(path: Path, buf_size: int = SHA1_BUF_SIZE) -> str:
    logger.debug(f'calc sha1sum for {path}')
    sha1 = hashlib.sha1()
    with path.open('rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def load_module(local_filename):
    module_path = Path.cwd() / local_filename
    if not module_path.exists():
        raise Exception(f'Module file {module_path} does not exist')

    # No longer needed due to sys.modules line below.
    # Make sure any local imports in the config script work.
    sys.path.append(str(module_path.parent))
    module_name = Path(local_filename).stem

    try:
        # See: https://stackoverflow.com/a/50395128/54557
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except SyntaxError as se:
        print(f'Bad syntax in config file {module_path}')
        raise

    return module


def load_task_ctrls(filename):
    task_ctrl_module = load_module(filename)
    task_ctrls = []
    functions = [o for o in [getattr(task_ctrl_module, m) for m in dir(task_ctrl_module)]
                 if inspect.isfunction(o)]
    for func in functions:
        if hasattr(func, 'is_remake_task_control') and func.is_remake_task_control:
            task_ctrl = func()
            if not isinstance(task_ctrl, TaskControl):
                raise Exception(f'{task_ctrl} is not a TaskControl (defined in {func})')
            task_ctrls.append(task_ctrl)
    if not task_ctrls:
        raise Exception(f'No task controls defined in {filename}')

    return task_ctrls

    # TODO: delete.
    # if not hasattr(task_ctrl_module, 'REMAKE_TASK_CTRL_FUNC'):
    #     raise Exception(f'No REMAKE_TASK_CTRL_FUNC defined in {filename}')
    # task_ctrl_func_name = task_ctrl_module.REMAKE_TASK_CTRL_FUNC
    # if not hasattr(task_ctrl_module, task_ctrl_func_name):
    #     raise Exception(f'No function {task_ctrl_func_name} defined in {filename}')
    # task_ctrl_func = getattr(task_ctrl_module, task_ctrl_func_name)
    # logger.debug(f'got task_ctrl_func: {task_ctrl_func}')
    # task_ctrl = task_ctrl_func()
    # if not isinstance(task_ctrl, TaskControl):
    #     raise Exception(f'{task_ctrl} is not a TaskControl')
    # return task_ctrl