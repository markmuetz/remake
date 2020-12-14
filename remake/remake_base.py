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
        # This is needed for when MultiprocExecutor makes its own Remakes in worker procs.
        if multiprocessing.current_process().name == 'MainProcess':
            if name in Remake.remakes:
                # Can happen on ipython run remakefile.
                logger.info(f'Remake {name} added twice')
            Remake.remakes[name] = self
        else:
            logger.debug(f'Process {multiprocessing.current_process().name}')
            logger.debug(Remake.current_remake)
            logger.debug(Remake.remakes)

        Remake.current_remake[multiprocessing.current_process().name] = self

        self.config = None
        self.task_ctrl = TaskControl(name)
        self.rules = []
        self.tasks = TaskQuerySet(task_ctrl=self.task_ctrl)

    @property
    def name(self):
        return self.task_ctrl.name

    @property
    def pending_tasks(self):
        return self.task_ctrl.pending_tasks

    @property
    def remaining_tasks(self):
        return self.task_ctrl.remaining_tasks

    @property
    def completed_tasks(self):
        return self.task_ctrl.completed_tasks

    def task_status(self, task):
        return self.task_ctrl.statuses.task_status(task)

    def rerun_required(self):
        assert self.finalized
        return self.task_ctrl.rescan_tasks or self.task_ctrl.pending_tasks

    def configure(self, print_reasons, executor, display):
        self.task_ctrl.print_reasons = print_reasons
        self.task_ctrl.set_executor(executor)
        if display == 'print_status':
            self.task_ctrl.display_func = self.task_ctrl.__class__.print_status
        elif display == 'task_dag':
            from remake.experimental.networkx_displays import display_task_status
            self.task_ctrl.display_func = display_task_status
        elif display:
            raise Exception(f'display {display} not recognized')

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

    def list_rules(self):
        return self.rules

    def find_tasks(self, task_path_hash_keys):
        tasks = TaskQuerySet([], self.task_ctrl)
        for task_path_hash_key in task_path_hash_keys:
            if len(task_path_hash_key) == 40:
                tasks.append(self.task_ctrl.task_from_path_hash_key[task_path_hash_key])
            else:
                # TODO: Make less bad.
                # I know this is terribly inefficient!
                _tasks = []
                for k, v in self.task_ctrl.task_from_path_hash_key.items():
                    if k[:len(task_path_hash_key)] == task_path_hash_key:
                        _tasks.append(v)
                if len(_tasks) == 0:
                    raise KeyError(task_path_hash_key)
                elif len(_tasks) > 1:
                    raise KeyError(f'{task_path_hash_key} matches multiple keys')
                tasks.append(_tasks[0])
        return tasks

    def list_tasks(self, tfilter, rule):
        tasks = TaskQuerySet([t for t in self.tasks], self.task_ctrl)
        if tfilter:
            filter_kwargs = dict([kv.split('=') for kv in tfilter.split(',')])
            tasks = tasks.filter(cast_to_str=True, **filter_kwargs)
        if rule:
            tasks = tasks.in_rule(rule)
        return tasks

    def list_files(self, filetype, exists):
        if filetype is None:
            files = sorted(set(self.task_ctrl.input_task_map.keys()) | set(self.task_ctrl.output_task_map.keys()))
        elif filetype == 'input':
            files = sorted(self.task_ctrl.input_task_map.keys())
        elif filetype == 'output':
            files = sorted(self.task_ctrl.output_task_map.keys())
        elif filetype == 'input_only':
            files = sorted(set(self.task_ctrl.input_task_map.keys()) - set(self.task_ctrl.output_task_map.keys()))
        elif filetype == 'output_only':
            files = sorted(set(self.task_ctrl.output_task_map.keys()) - set(self.task_ctrl.input_task_map.keys()))
        elif filetype == 'inout':
            files = sorted(set(self.task_ctrl.output_task_map.keys()) & set(self.task_ctrl.input_task_map.keys()))
        else:
            raise Exception(f'Unknown {filetype=}')
        if exists:
            files = [f for f in files if f.exists()]
        return files

    def task_info(self, task_path_hash_keys):
        assert self.finalized
        info = {}
        tasks = self.find_tasks(task_path_hash_keys)
        for task_path_hash_key, task in zip(task_path_hash_keys, tasks):
            task_md = self.task_ctrl.metadata_manager.task_metadata_map[task]
            status = self.task_ctrl.statuses.task_status(task)
            info[task_path_hash_key] = (task, task_md, status)
        return info

    def file_info(self, filenames):
        info = {}
        for filepath in (Path(fn).absolute() for fn in filenames):
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
            info[filepath] = path_md, produced_by_task, used_by_tasks
        return info

    @property
    def finalized(self):
        return self.task_ctrl.finalized

    def reset(self):
        self.task_ctrl.reset()
        return self

    def finalize(self):
        self.task_ctrl.finalize()
        Remake.current_remake[multiprocessing.current_process().name] = None
        return self
