import difflib
import inspect
from hashlib import sha1
from logging import getLogger
from pathlib import Path
from timeit import default_timer as timer

from remake.flags import RemakeOn
from remake.setup_logging import add_file_logging, remove_file_logging
from remake.special_paths import map_special_paths
from remake.remake_exceptions import FileNotCreated

logger = getLogger(__name__)


def tmp_atomic_path(p):
    return p.parent / ('.remake.tmp.' + p.name)


def map_val(v):
    return f"'{v}'" if isinstance(v, str) else v


class BaseTask:
    def __init__(self, task_ctrl):
        self.task_ctrl = task_ctrl
        self.task_md = None
        self.check_path_metadata = True

    def add_metadata(self, task_md):
        self.task_md = task_md

    def update_status(self, status):
        self.task_md.update_status(status.upper())

    @property
    def status(self):
        return self.task_ctrl.statuses.task_status(self)

    @property
    def next_tasks(self):
        return list(self.task_ctrl.task_dag.successors(self))

    @property
    def prev_tasks(self):
        return list(self.task_ctrl.task_dag.predecessors(self))


class Task(BaseTask):
    task_func_cache = {}

    def __init__(self, task_ctrl, func, inputs, outputs,
                 *, force=False, depends_on=tuple()):
        super().__init__(task_ctrl)
        # self.remake_on = True
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
        if inspect.isfunction(self.func):
            self.func_bytecode = self.func.__code__.co_code
        elif inspect.ismethod(self.func):
            self.func_bytecode = self.func.__func__.__code__.co_code
        elif inspect.isclass(self.func):
            self.func_bytecode = self.func.__call__.__func__.__code__.co_code
        else:
            raise Exception(f'func is not a function, method or class: {self.func} -- type: {type(self.func)}')
        self.force = force

        if not outputs:
            raise Exception('outputs must be set')

        self.inputs = {k: Path(v).absolute() for k, v in inputs.items()}
        self.outputs = {k: Path(v).absolute() for k, v in outputs.items()}
        self.special_inputs = map_special_paths(self.task_ctrl.special_paths, self.inputs)
        self.special_outputs = map_special_paths(self.task_ctrl.special_paths, self.outputs)
        self.result = None
        self.rerun_on_mtime = True
        self.tmp_outputs = {}
        self.logger = None
        self._path_hash_key = None

    def __repr__(self):
        return str(self)

    def __str__(self):
        if hasattr(self, 'var_matrix'):
            args = []
            for k in self.var_matrix.keys():
                if isinstance(k, str):
                    args.append(f'{k}={map_val(getattr(self, k))}')
                elif isinstance(k, tuple):
                    for kk in k:
                        args.append(f'{kk}={map_val(getattr(self, kk))}')
            args = ', '.join(args)
            return f'{self.path_hash_key()[:10]} {self.__class__.__name__}({args})'
        else:
            return f'{self.path_hash_key()[:10]} {self.__class__.__name__}()'

    def can_run(self):
        can_run = True
        for input_path in self.inputs.values():
            if not input_path.exists():
                can_run = False
                break
        return can_run

    def diff(self):
        if self.task_md and self.task_md.metadata:
            func_last = self.task_md.metadata['func_source']
            func = self.func_source
            if func_last == func:
                return None
            diff = list(difflib.ndiff(func_last.split('\n'),
                                      func.split('\n')))
            return diff
        return None

    def complete(self):
        for output in self.outputs.values():
            if not output.exists():
                return False
        return True

    def path_hash_key(self):
        if not self._path_hash_key:
            h = sha1(self.func.__code__.co_name.encode())
            for input_path in self.special_inputs.values():
                h.update(str(input_path).encode())
            for output_path in self.special_outputs.values():
                h.update(str(output_path).encode())
            self._path_hash_key = h.hexdigest()
        return self._path_hash_key

    def run_task_rule(self, force=False):
        self.task_ctrl.run_task(self, force=force)

    def run(self, use_task_control=True):
        if use_task_control:
            self.task_ctrl.run_requested([self])
            return

        logger.debug(f'running {repr(self)}')
        if not self.can_run():
            raise Exception('Not all files required for task exist')

        self.task_md.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.update_status('RUNNING')

        logger_name = f'remake.task.{self.__class__.__name__}'
        try:
            for output_dir in set([o.parent for o in self.outputs.values()]):
                output_dir.mkdir(parents=True, exist_ok=True)
            logger.debug('atomic_write: make temp paths')
            self.tmp_outputs = {k: tmp_atomic_path(v) for k, v in self.outputs.items()}

            start = timer()

            orig_outputs = self.outputs
            self.outputs = self.tmp_outputs

            self.logger = getLogger(logger_name)
            add_file_logging(self.task_md.log_path, 'DEBUG', logger_name)

            self.logger.debug(f'Running: {self}')
            self.result = self.func(self)
            self.logger.debug(f'Completed: {self}')

            self.outputs = orig_outputs

            logger.debug(f'{self} completed in {timer() - start:.2f}s:'
                         f' {[o.name for o in self.outputs.values()]}')
            for output in self.tmp_outputs.values():
                if not output.exists():
                    raise FileNotCreated(f'func {output} not created')
            tmp_paths = self.tmp_outputs.values()
            for tmp_path, path in zip(tmp_paths, self.outputs.values()):
                tmp_path.rename(path)
            self._post_run_with_content_check()
        except (Exception, FileNotCreated):
            self.update_status('ERROR')
            if self.logger:
                self.logger.exception('')
            logger.exception('')
            raise
        finally:
            if self.logger:
                remove_file_logging(self.task_md.log_path, logger_name)
                self.logger = None

        return self

    def _post_run_with_content_check(self):
        logger.debug('post run content checks')
        self.task_md.generate_metadata()
        self.task_md.write_task_metadata()

        logger.debug('post run content checks extra_checks')
        requires_rerun = self.task_md.task_requires_rerun()
        if not self.check_path_metadata and requires_rerun & RemakeOn.INPUTS_CHANGED:
            requires_rerun &= ~RemakeOn.INPUTS_CHANGED

        assert not requires_rerun


class RescanFileTask(BaseTask):
    def __init__(self, task_ctrl, filepath, path_md, pathtype):
        super().__init__(task_ctrl)
        self.filepath = filepath
        self.path_md = path_md
        self.pathtype = pathtype
        self.inputs = {'filepath': Path(filepath).absolute()}
        self.special_inputs = map_special_paths(task_ctrl.special_paths, self.inputs)
        self.outputs = {}
        self.special_outputs = {}
        self.force = False

    def __str__(self):
        return f'{self.path_hash_key()[:10]} {self.__class__.__name__}({self.filepath})'

    def can_run(self):
        can_run = True
        for input_path in self.inputs.values():
            if not input_path.exists():
                can_run = False
                break
        return can_run

    def complete(self):
        metadata_has_changed = self.path_md.compare_path_with_previous()
        return not metadata_has_changed

    def path_hash_key(self):
        h = sha1()
        for input_path in self.inputs.values():
            h.update(str(input_path).encode())
        return h.hexdigest()

    def run(self, force=False, use_task_control=True):
        self.update_status('RUNNING')
        if use_task_control:
            self.task_ctrl.run_requested([self])
            return
        metadata_has_changed = self.path_md.compare_path_with_previous()
        assert metadata_has_changed
        self.path_md.gen_sha1hex()
        if self.path_md.metadata and self.path_md.metadata['sha1hex'] != self.path_md.new_metadata['sha1hex']:
            # I'm not sure this logic still holds when some tasks are marked cannot_run.
            # if self.pathtype == 'inout':
            #     # TODO: this needs to be overridable with a config option.
            #     raise Exception(f'Content changed of inout: {self.path_md.path}')
            # else:
            #     logger.debug(f'Content changed of in: {self.path_md.path}')
            logger.debug(f'Content changed of in: {self.path_md.path}')

        self.path_md.write_new_metadata()
