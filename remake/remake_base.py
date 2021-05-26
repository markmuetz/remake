from logging import getLogger
import multiprocessing
from pathlib import Path
import random
import traceback

import networkx as nx

from remake.remake_exceptions import RemakeError
from remake.task import Task
from remake.task_control import TaskControl
from remake.task_query_set import TaskQuerySet
from remake.setup_logging import setup_stdout_logging

logger = getLogger(__name__)


class Remake:
    remakes = {}
    current_remake = {}

    def __init__(self, name=None, config=None):
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

        self.config = config
        self.task_ctrl = TaskControl(name, config)
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

    def short_status(self):
        logger.info(f'Status (complete/rescan/pending/remaining/cannot run): '
                    f'{len(self.completed_tasks)}/{len(self.task_ctrl.rescan_tasks)}/'
                    f'{len(self.pending_tasks)}/{len(self.remaining_tasks)}/{len(self.task_ctrl.cannot_run_tasks)}')

    def display_task_dag(self):
        from remake.experimental.networkx_displays import display_task_status
        import matplotlib.pyplot as plt
        display_task_status(self.task_ctrl)
        plt.show()

    def run_all(self, force=False):
        self.task_ctrl.run_all(force=force)

    def run_one(self, force=False):
        try:
            task = next(self.task_ctrl.get_next_pending())
            self.run_requested([task], force=force)
        except StopIteration:
            pass

    def run_random(self, force=False):
        task = random.choice(list(self.task_ctrl.pending_tasks))
        self.run_requested([task], force=force)

    def run_requested(self, requested, force=False, handle_dependencies=False):
        # Work out whether it's possible to run requested tasks.
        ancestors = self.all_ancestors(requested)
        rerun_required_ancestors = ancestors & (self.pending_tasks |
                                                self.remaining_tasks)
        missing_tasks = rerun_required_ancestors - set(requested)
        if missing_tasks:
            logger.debug(f'{len(missing_tasks)} need to be added')
            if not handle_dependencies:
                logger.error('Impossible to run requested tasks')
                raise RemakeError('Cannot run with requested tasks. Use --handle-dependencies to fix.')
            else:
                requested = list(rerun_required_ancestors)
        requested = self.task_ctrl.rescan_tasks + requested
        self.task_ctrl.run_requested(requested, force=force)

    def list_rules(self):
        return self.rules

    def find_task(self, task_path_hash_key):
        if isinstance(task_path_hash_key, Task):
            return task_path_hash_key
        else:
            return self.find_tasks([task_path_hash_key])[0].task

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

    def list_tasks(self, tfilter=None, rule=None, requires_rerun=False,
                   uses_file=None, produces_file=None,
                   ancestor_of=None, descendant_of=None):
        tasks = TaskQuerySet([t for t in self.tasks], self.task_ctrl)
        if tfilter:
            tasks = tasks.filter(cast_to_str=True, **tfilter)
        if rule:
            tasks = tasks.in_rule(rule)
        if uses_file:
            uses_file = Path(uses_file).absolute()
            tasks = [t for t in tasks if uses_file in t.inputs.values()]
        if produces_file:
            produces_file = Path(produces_file).absolute()
            tasks = [t for t in tasks if produces_file in t.outputs.values()]
        if ancestor_of:
            ancestor_of = self.find_task(ancestor_of)
            ancestor_tasks = self.ancestors(ancestor_of)
            tasks = sorted(ancestor_tasks & set(tasks), key=self.task_ctrl.sorted_tasks.get)
        if descendant_of:
            descendant_of = self.find_task(descendant_of)
            descendant_tasks = self.descendants(descendant_of)
            tasks = sorted(descendant_tasks & set(tasks), key=self.task_ctrl.sorted_tasks.get)
        if requires_rerun:
            tasks = [t for t in tasks
                     if self.task_ctrl.statuses.task_status(t) in ['pending', 'remaining']]

        return TaskQuerySet(tasks, self.task_ctrl)

    def all_descendants(self, tasks):
        descendants = set()
        for task in tasks:
            if task in descendants:
                continue
            descendants |= self.descendants(task)
        return descendants

    def all_ancestors(self, tasks):
        ancestors = set()
        for task in tasks:
            if task in ancestors:
                continue
            ancestors |= self.ancestors(task)
        return ancestors

    def descendants(self, task):
        return set(nx.bfs_tree(self.task_ctrl.task_dag, task))

    def ancestors(self, task):
        return set(nx.bfs_tree(self.task_ctrl.task_dag, task, reverse=True))

    def list_files(self, filetype=None, exists=False,
                   produced_by_rule=None, used_by_rule=None,
                   produced_by_task=None, used_by_task=None):
        input_paths = set(self.task_ctrl.input_task_map.keys())
        output_paths = set(self.task_ctrl.output_task_map.keys())
        input_only_paths = input_paths - output_paths
        output_only_paths = output_paths - input_paths
        inout_paths = input_paths & output_paths
        files = input_paths | output_only_paths

        if filetype is None:
            files = sorted(files)
        elif filetype == 'input_only':
            files = sorted(input_only_paths)
        elif filetype == 'output_only':
            files = sorted(output_only_paths)
        elif filetype == 'input':
            files = sorted(input_paths)
        elif filetype == 'output':
            files = sorted(output_paths)
        elif filetype == 'inout':
            files = sorted(inout_paths)
        else:
            raise ValueError(f'Unknown {filetype=}')
        if exists:
            files = [f for f in files if f.exists()]
        if used_by_rule:
            _files = set()
            for f in files:
                if f not in self.task_ctrl.input_task_map:
                    continue
                for t in self.task_ctrl.input_task_map[f]:
                    if t.__class__.__name__ == used_by_rule:
                        _files.add(f)
            files = sorted(_files)
        if produced_by_rule:
            _files = set()
            for f in files:
                if f not in self.task_ctrl.output_task_map:
                    continue
                t = self.task_ctrl.output_task_map[f]
                if t.__class__.__name__ == produced_by_rule:
                    _files.add(f)
            files = sorted(_files)
        if used_by_task:
            used_by_task = self.find_task(used_by_task)
            _files = set()
            for f in files:
                if f not in self.task_ctrl.input_task_map:
                    continue
                for t in self.task_ctrl.input_task_map[f]:
                    if t is used_by_task:
                        _files.add(f)
            files = sorted(_files)
        if produced_by_task:
            produced_by_task = self.find_task(produced_by_task)
            _files = set()
            for f in files:
                if f not in self.task_ctrl.output_task_map:
                    continue
                t = self.task_ctrl.output_task_map[f]
                if t is produced_by_task:
                    _files.add(f)
            files = sorted(_files)

        filelist = []
        for file in files:
            if file in input_only_paths:
                ftype = 'input-only'
            elif file in output_only_paths:
                ftype = 'output-only'
            elif file in inout_paths:
                ftype = 'inout'
            filelist.append((file, ftype, file.exists()))

        return filelist

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
