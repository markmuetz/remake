from collections import Mapping
import inspect
from hashlib import sha1
from logging import getLogger
from pathlib import Path
from timeit import default_timer as timer

from remake.flags import RemakeOn

logger = getLogger(__name__)


def tmp_atomic_path(p):
    return p.parent / ('.remake.tmp.' + p.name)


class Task:
    task_func_cache = {}

    def __init__(self, func, inputs, outputs, func_args=tuple(), func_kwargs=None,
                 *, atomic_write=True):
        if func_kwargs is None:
            func_kwargs = {}
        if hasattr(func, 'is_remake_wrapped') and func.is_remake_wrapped:
            self.remake_required = func
            depends_on = func.depends_on
            self.remake_on = func.remake_on
            func = func.remake_func
        else:
            self.remake_required = False
            self.remake_on = True
            depends_on = []
        self.depends_on_sources = []
        for depend_obj in depends_on:
            # depends_on can be any object which inspect.getsource can handle
            # class, method, functions are the most likely to be used.
            # Note, you cannot get bytecode of classes.
            if depend_obj in Task.task_func_cache:
                self.depends_on_sources.append(Task.task_func_cache[depend_obj])
            else:
                try:
                    depend_func_source = inspect.getsource(depend_obj)
                except OSError:
                    logger.error(f'Cannot retrieve source for {depend_obj}')
                    raise
                self.depends_on_sources.append(depend_func_source)
                Task.task_func_cache[depend_obj] = depend_func_source

        self.depends_on = depends_on

        if not callable(func):
            raise ValueError(f'{func} is not callable')

        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs
        if self.func in Task.task_func_cache:
            self.func_source = Task.task_func_cache[self.func]
        else:
            all_func_source_lines, _ = inspect.getsourcelines(self.func)
            # Ignore any decorators.
            # Even after unwrapping the decorated function, the decorator line(s) are returned by
            # inspect.getsource...
            # Filter them out by discarding any lines that start with '@'.
            func_source_lines = []
            for line in all_func_source_lines:
                if not line.lstrip():
                    func_source_lines.append(line)
                elif line.lstrip()[0] != '@':
                    func_source_lines.append(line)

            self.func_source = ''.join(func_source_lines)
            Task.task_func_cache[self.func] = self.func_source

        # Faster; no need to cache.
        try:
            self.func_bytecode = self.func.__code__.co_code
        except AttributeError:
            # Will be hit if func is a callable class.
            assert inspect.isclass(self.func), f'{self.func} not a class'
            self.func_bytecode = self.func.__call__.__code__.co_code
        self.atomic_write = atomic_write

        if not outputs:
            raise Exception('outputs must be set')

        if isinstance(inputs, Mapping):
            self.inputs_dict = {k: Path(v).absolute() for k, v in inputs.items()}
            self.inputs = [Path(i).absolute() for i in inputs.values()]
        else:
            self.inputs_dict = None
            self.inputs = [Path(i).absolute() for i in inputs]
        if isinstance(outputs, Mapping):
            self.outputs_dict = {k: Path(v).absolute() for k, v in outputs.items()}
            self.outputs = [Path(o).absolute() for o in outputs.values()]
        else:
            self.outputs_dict = None
            self.outputs = [Path(o).absolute() for o in outputs]
        self.result = None
        self.rerun_on_mtime = True
        self.tmp_outputs = []

    def __repr__(self):
        return f'{self.__class__}({self.func.__code__.co_name}, {self.inputs}, {self.outputs})'

    def __str__(self):
        return f'{self.__class__.__name__}' \
               f'({self.func.__code__.co_name}, {[f.name for f in self.inputs]}, {[f.name for f in self.outputs]})'

    def can_run(self):
        can_run = True
        for input_path in self.inputs:
            if not input_path.exists():
                can_run = False
                break
        return can_run

    def requires_rerun(self):
        rerun = RemakeOn.NOT_NEEDED
        earliest_output_path_mtime = float('inf')
        for output in self.outputs:
            if not Path(output).exists():
                rerun |= RemakeOn.MISSING_OUTPUT
                break
            earliest_output_path_mtime = min(earliest_output_path_mtime,
                                             output.stat().st_mtime)
        if self.rerun_on_mtime and not rerun:
            latest_input_path_mtime = 0
            for input_path in self.inputs:
                latest_input_path_mtime = max(latest_input_path_mtime,
                                              input_path.stat().st_mtime)
            if latest_input_path_mtime > earliest_output_path_mtime:
                rerun |= RemakeOn.OLDER_OUTPUT

        return rerun

    def complete(self):
        for output in self.outputs:
            if not output.exists():
                return False
        return True

    def path_hash_key(self):
        h = sha1(self.func.__code__.co_name.encode())
        for input_path in self.inputs:
            h.update(str(input_path).encode())
        for output_path in self.outputs:
            h.update(str(output_path).encode())
        return h.hexdigest()

    def run(self, force=False):
        logger.debug(f'running {repr(self)}')
        if not self.can_run():
            raise Exception('Not all files required for task exist')

        if self.requires_rerun() or force:
            logger.debug(f'requires_rerun or force')
            for output_dir in set([o.parent for o in self.outputs]):
                output_dir.mkdir(parents=True, exist_ok=True)
            inputs = self.inputs_dict if self.inputs_dict else self.inputs
            if self.atomic_write:
                logger.debug(f'atomic_write: make temp paths')
                if self.outputs_dict:
                    self.tmp_outputs = {k: tmp_atomic_path(v) for k, v in self.outputs_dict.items()}
                else:
                    self.tmp_outputs = [tmp_atomic_path(v) for v in self.outputs]
            else:
                self.tmp_outputs = self.outputs_dict if self.outputs_dict else self.outputs

            logger.debug(f'run func {self.func}, {self.func_args}, {self.func_kwargs}')
            start = timer()
            self.result = self.func(inputs, self.tmp_outputs, *self.func_args, **self.func_kwargs)
            logger.debug(f'run func {self.func} completed in {timer() - start:.2f}s:'
                         f' {[o.name for o in self.outputs]}')
            if self.atomic_write:
                logger.debug(f'atomic_write: rename temp paths')
                if self.outputs_dict:
                    tmp_paths = self.tmp_outputs.values()
                else:
                    tmp_paths = self.tmp_outputs
                for tmp_path, path in zip(tmp_paths, self.outputs):
                    tmp_path.rename(path)

            for output in self.outputs:
                if not output.exists():
                    raise Exception(f'func {output} not created')
        else:
            logger.debug(f'already exist: {self.outputs}')

        return self

