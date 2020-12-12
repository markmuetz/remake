from collections import defaultdict
import functools
from logging import getLogger
from pathlib import Path
from typing import List

import networkx as nx

from remake.task import Task, RescanFileTask
from remake.metadata import MetadataManager
from remake.flags import RemakeOn
from remake.executor.singleproc_executor import SingleprocExecutor
from remake.executor.multiproc_executor import MultiprocExecutor
from remake.executor.slurm_executor import SlurmExecutor

logger = getLogger(__name__)


class TaskStatuses:
    def __init__(self):
        self._completed_tasks = set()
        self._pending_tasks = set()
        self._ordered_pending_tasks = []
        self._running_tasks = set()
        self._remaining_tasks = set()
        self._task_status = {}

    def reset(self):
        self.__init__()

    def add_task(self, task, status):
        getattr(self, f'_{status}_tasks').add(task)
        if status == 'pending':
            self._ordered_pending_tasks.append(task)
        self._task_status[task] = status

    def update_task(self, task, old_status, new_status):
        old_tasks = getattr(self, f'_{old_status}_tasks')
        new_tasks = getattr(self, f'_{new_status}_tasks')
        assert task in old_tasks
        assert self._task_status[task] in old_status
        old_tasks.remove(task)
        new_tasks.add(task)
        if old_status == 'pending':
            self._ordered_pending_tasks.remove(task)
        if new_status == 'pending':
            self._ordered_pending_tasks.append(task)
        self._task_status[task] = new_status

    def task_status(self, task):
        return self._task_status[task]

    @property
    def completed_tasks(self):
        return self._completed_tasks

    @property
    def pending_tasks(self):
        return self._pending_tasks

    @property
    def ordered_pending_tasks(self):
        return self._ordered_pending_tasks

    @property
    def running_tasks(self):
        return self._running_tasks

    @property
    def remaining_tasks(self):
        return self._remaining_tasks

    def print_status(self):
        print(f'  completed: {len(self.completed_tasks)}')
        print(f'  pending  : {len(self.pending_tasks)}')
        print(f'  running  : {len(self.running_tasks)}')
        print(f'  remaining: {len(self.remaining_tasks)}')
        print(f'  all      : {len(self._task_status)}')


def check_finalized(finalized):
    def _check_finalized(method):
        @functools.wraps(method)
        def wrapper(task_ctrl, *args, **kwargs):
            if task_ctrl.finalized != finalized:
                raise Exception(f'Call {method} when TaskControl finalized={finalized}')
            return method(task_ctrl, *args, **kwargs)

        return wrapper

    return _check_finalized


# noinspection PyAttributeOutsideInit
class TaskControl:
    def __init__(self, filename: str, dependencies: List['TaskControl'] = None, *,
                 remake_on: RemakeOn = RemakeOn.ANY_STANDARD_CHANGE,
                 dotremake_dir='.remake',
                 print_reasons=False):
        self.filename = filename
        self.dependencies = dependencies
        self.path = Path(filename).absolute()
        self.name = self.path.stem
        self.remake_on = remake_on
        self.print_reasons = print_reasons
        self.extra_checks = True
        self.tasks = []

        # Get added to as new tasks are added.
        self.output_task_map = {}
        self.input_task_map = defaultdict(list)
        self.task_from_path_hash_key = {}
        self.dotremake_dir = Path(dotremake_dir)
        self.dotremake_dir.mkdir(parents=True, exist_ok=True)
        self.set_executor('singleproc')

        self.reset()

    def reset(self):
        self.metadata_manager = MetadataManager(self.name, self.dotremake_dir)
        self.finalized = False

        # Generated by self.finalize()
        self.input_paths = set()
        self.output_paths = set()
        self.input_tasks = set()

        self.task_dag = nx.DiGraph()
        self.sorted_tasks = []
        self.rescan_tasks = []

        self.statuses = TaskStatuses()

        self.tasks_at_level = {}

        self._dag_built = False
        return self

    def set_executor(self, executor):
        if executor == 'singleproc':
            self.executor = SingleprocExecutor()
        elif executor == 'slurm':
            self.executor = SlurmExecutor(self)
        else:
            logger.warning('MULTIPROC EXECUTOR DOES NOT WORK AND MAY SLOW YOUR COMPUTER!')
            # r = input('PRESS y to continue: ')
            # if r != 'y':
            #     raise Exception('Fix MultiprocExecutor')
            self.executor = MultiprocExecutor(self)

    @property
    def completed_tasks(self):
        return self.statuses.completed_tasks

    @property
    def pending_tasks(self):
        return self.statuses.pending_tasks

    @property
    def running_tasks(self):
        return self.statuses.running_tasks

    @property
    def remaining_tasks(self):
        return self.statuses.remaining_tasks

    @check_finalized(False)
    def add(self, task):
        for output in task.outputs.values():
            if output in self.output_task_map:
                raise Exception(f'Trying to add {output} twice')
        task_path_hash_key = task.path_hash_key()
        if task_path_hash_key in self.task_from_path_hash_key:
            raise Exception(f'Trying to add {task} twice')
        self.task_from_path_hash_key[task_path_hash_key] = task

        self.tasks.append(task)
        for input_path in task.inputs.values():
            self.input_task_map[input_path].append(task)
        for output in task.outputs.values():
            self.output_task_map[output] = task

        task_md = self.metadata_manager.create_task_metadata(task)
        task.add_metadata(task_md)

        return task

    def _topogological_tasks(self):
        assert self._dag_built

        level = 0
        curr_tasks = set(self.input_tasks)
        visited = set()
        all_tasks = set()
        while curr_tasks:
            self.tasks_at_level[level] = sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0])
            next_tasks = set()
            for curr_task in sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0]):
                if curr_task in visited:
                    # Why not just check this earlier? nx method just returns True or False, whereas
                    # I can build up a bit more info to supply to user.
                    assert not nx.is_directed_acyclic_graph(self.task_dag), ('networkx and TaskControl disagree on DAG!'
                                                                             ' networkx thinks it is.')
                    # Cycle detected. Find offending tasks (there may be more cycles than these).
                    input_tasks = []
                    for task in self.tasks:
                        for input_path in task.inputs.values():
                            if input_path in curr_task.outputs.values():
                                input_tasks.append(task)

                    raise Exception(f'cycle detected in DAG:\n  {curr_task} produces input for\n  >' +
                                    '\n  >'.join([str(t) for t in input_tasks]) + '\n')
                visited.add(curr_task)
                can_yield = True
                # for prev_task in self.prev_tasks[curr_task]:
                for prev_task in self.task_dag.predecessors(curr_task):
                    if prev_task not in all_tasks:
                        can_yield = False
                        break
                if can_yield and curr_task not in all_tasks:
                    yield curr_task
                    all_tasks.add(curr_task)

                for next_task in self.task_dag.successors(curr_task):
                    next_tasks.add(next_task)
            curr_tasks = next_tasks
            level += 1

        if self.extra_checks:
            assert nx.is_directed_acyclic_graph(self.task_dag), ('networkx and TaskControl disagree on DAG!'
                                                                 ' networkx thinks it is not.')

    def task_requires_rerun(self, task, print_reasons=False):
        if isinstance(task, RescanFileTask):
            return task.requires_rerun()

        logger.debug('performing task file contents checks')

        for path in task.inputs.values():
            if not path.exists():
                continue
            path_md = self.metadata_manager.path_metadata_map[path]
            metadata_has_changed = path_md.compare_path_with_previous()
            if metadata_has_changed:
                self.gen_rescan_task(path)

        task_md = self.metadata_manager.task_metadata_map[task]
        task_md.generate_metadata()
        requires_rerun = task_md.task_requires_rerun()

        if requires_rerun:
            logger.debug(f'requires rerun: {requires_rerun}')
            for reason in task_md.rerun_reasons:
                logger.debug(f'  {reason}')
                if print_reasons:
                    logger.info(f'  --reason: {reason}')
        if print_reasons and not requires_rerun:
            logger.info(f'  --reason: not needed')
        return requires_rerun

    def gen_rescan_task(self, path):
        path = Path(path)
        path_md = self.metadata_manager.path_metadata_map[path]
        if path in self.input_task_map and path in self.output_task_map:
            pathtype = 'inout'
        elif path in self.input_task_map:
            pathtype = 'in'
        else:
            raise Exception('gen_rescan_task should never be called on output only path')
        rescan_task = RescanFileTask(self, path_md.path, path_md, pathtype)
        task_md = self.metadata_manager.create_task_metadata(rescan_task)
        rescan_task.add_metadata(task_md)
        self.rescan_tasks.append(rescan_task)
        for next_task in self.input_task_map[path]:
            if next_task is not rescan_task:
                self.task_dag.add_edge(rescan_task, next_task)
        return rescan_task

    @check_finalized(False)
    def finalize(self):
        logger.info(f'==={self.name}===')
        if not self.tasks:
            raise Exception('No tasks have been added')

        if self.dependencies:
            for dep in self.dependencies:
                dep.finalize()
                if not dep.tasks == dep.completed_tasks:
                    raise Exception(f'Dependency task control {dep.name} is not complete')

        logger.info('Build task DAG')
        self.build_task_DAG()

        missing_paths = [p for p in self.input_paths if not p.exists()]
        if missing_paths:
            for input_path in missing_paths:
                tasks = self.input_task_map[input_path]
                logger.error(f'No input file {input_path} exists or will be created (needed by {len(tasks)} tasks)')
            raise Exception(f'Not all input paths exist: {len(missing_paths)} missing')

        logger.info('Perform topological sort')
        # Can now perform a topological sort.
        self.sorted_tasks = list(self._topogological_tasks())
        # N.B. provides nicer ordering of tasks than using self.task_dag.
        # self.sorted_tasks = list(nx.topological_sort(self.task_dag))
        if self.extra_checks:
            logger.debug('performing extra checks on sorted tasks')
            assert len(self.sorted_tasks) == len(self.tasks)
            assert set(self.sorted_tasks) == set(self.tasks)

        logger.info('Assign status to tasks')
        self._assign_tasks()

        if self.extra_checks:
            logger.debug('performing extra checks on groups')
            all_tasks_assigned = (self.completed_tasks | self.pending_tasks | self.remaining_tasks ==
                                  set(self.tasks) and
                                  len(self.completed_tasks) + len(self.pending_tasks) + len(self.remaining_tasks) ==
                                  len(self.tasks))
            assert all_tasks_assigned, 'All tasks not assigned.'

        self.output_paths = set(self.output_task_map.keys()) - self.input_paths

        self.finalized = True
        return self

    def _assign_tasks(self):
        # Assign each task to one of three groups:
        # completed: task has been run and does not need to be rerun.
        # pending: task has been run and needs to be rerun.
        # remaining: task either needs to be rerun, or has previous tasks that need to be rerun.
        # import ipdb; ipdb.set_trace()
        for task in self.sorted_tasks:
            status = 'completed'
            requires_rerun = self.task_requires_rerun(task)

            if task.can_run() and (requires_rerun & self.remake_on) or task.force:
                status = 'pending'
                for prev_task in self.task_dag.predecessors(task):
                    if prev_task in self.pending_tasks or prev_task in self.remaining_tasks:
                        status = 'remaining'
                        break
            else:
                for prev_task in self.task_dag.predecessors(task):
                    if prev_task in self.pending_tasks or prev_task in self.remaining_tasks:
                        status = 'remaining'
                        break

            logger.debug(f'  task status: {status} - {task.path_hash_key()}')
            self.statuses.add_task(task, status)

    def build_task_DAG(self):
        if self._dag_built:
            logger.info('DAG already built')
            return

        # Work out whether it is possible to create a run schedule and find initial tasks.
        # Fill in self.prev_tasks and self.next_tasks; these hold the information about the
        # task DAG.
        for task in self.tasks:
            is_input_task = True
            self.task_dag.add_node(task)

            for input_path in task.inputs.values():
                if input_path in self.output_task_map:
                    is_input_task = False
                    # Every output is created by only one task.
                    input_task = self.output_task_map[input_path]
                    self.task_dag.add_edge(input_task, task)
                    if input_task not in self.task_dag.predecessors(task):
                        task.__class__.prev_rules.add(input_task.__class__)
                        input_task.__class__.next_rules.add(task.__class__)
                else:
                    self.input_paths.add(input_path)
            if is_input_task:
                self.input_tasks.add(task)

        self._dag_built = True

    def get_next_pending(self):
        while self.rescan_tasks or self.pending_tasks or self.remaining_tasks:
            if self.rescan_tasks:
                yield self.rescan_tasks.pop(0)
            elif not self.pending_tasks:
                yield None
            else:
                yield self.statuses.ordered_pending_tasks[0]

    def enqueue_task(self, task, force=False):
        if task is None:
            raise Exception('No task to enqueue')
        requires_rerun = self.task_requires_rerun(task, print_reasons=self.print_reasons)
        if force or task.force or requires_rerun & self.remake_on:
            if not isinstance(task, RescanFileTask):
                status = self.statuses.task_status(task)
                self.statuses.update_task(task, status, 'running')
            logger.debug(f'enqueue task (force={force}, requires_rerun={requires_rerun}): {task}')

            try:
                self.executor.enqueue_task(task)
            except Exception as e:
                logger.error(f'TaskControl: {self.name}')
                logger.error(e)
                task.task_md.update_status('ERROR')
                self.statuses.update_task(task, 'running', status)
                raise
            logger.debug(f'enqueued task: {task}')
            return True
        else:
            logger.debug(f'no longer requires enqueued: {task}')
            logger.info(f'  -> task run not needed')
            # TODO: at this point the DAG could be rescanned, and any downstream tasks could be marked as completed.
            return False

    def task_complete(self, task):
        assert task.complete(), 'task not complete'
        logger.debug(f'add completed task: {task.path_hash_key()}')
        if not isinstance(task, RescanFileTask) and task in self.running_tasks:
            self.statuses.update_task(task, 'running', 'completed')

        for next_task in self.task_dag.successors(task):
            if next_task in self.completed_tasks:
                continue
            add_to_pending = True
            # Make sure all previous tasks have been run.
            for prev_tasks in self.task_dag.predecessors(next_task):
                if prev_tasks not in self.completed_tasks:
                    add_to_pending = False
                    break
            if add_to_pending:
                logger.debug(f'adding new pending task: {next_task.path_hash_key()}')
                self.statuses.update_task(next_task, self.statuses.task_status(next_task), 'pending')

    @check_finalized(True)
    def run(self, *, requested_tasks=None, force=False):
        if self.executor.handles_dependencies:
            # Work here is done, just enqueue all pending and remaining tasks:
            if requested_tasks:
                tasks = requested_tasks
            else:
                remaining_tasks = sorted(self.remaining_tasks, key=lambda t: self.sorted_tasks.index(t))
                tasks = self.rescan_tasks + self.statuses.ordered_pending_tasks + remaining_tasks

            for task in tasks:
                self.executor.enqueue_task(task)
            self.executor.finish()
        else:
            if force:
                if requested_tasks:
                    tasks = sorted(requested_tasks, key=lambda x: self.sorted_tasks.index(x))
                    len_tasks = len([t for t in tasks if isinstance(t, Task)])
                    def task_index(t): return tasks.index(t)
                else:
                    tasks = [t for t in self.rescan_tasks + self.sorted_tasks]
                    len_tasks = len(self.sorted_tasks)
                    def task_index(t): return tasks.index(t)
            else:
                if requested_tasks:
                    tasks = sorted(requested_tasks, key=lambda x: self.sorted_tasks.index(x))
                    len_tasks = len(requested_tasks)
                    def task_index(t): return tasks.index(t)
                else:
                    tasks = self.get_next_pending()
                    len_tasks = len(self.sorted_tasks)
                    def task_index(t): return self.sorted_tasks.index(t)

            try:
                for task in tasks:
                    task_to_run = task
                    # Getting this working for both requested_tasks and normal is tricky!
                    # This works, but the logic is not easy to follow. It has to work for both
                    # tasks = self.get_next_pending() and a list-like obj.
                    # It has to work for different executor types.
                    # Maybe it would be clearer if I split this into 2 methods, one for requested_tasks
                    # and one for get_next_pending.
                    while task_to_run or not self.executor.can_accept_task():
                        if task_to_run and self.executor.can_accept_task():
                            if not isinstance(task_to_run, RescanFileTask):
                                logger.info(f'{task_index(task_to_run) + 1}/{len_tasks}:'
                                            f' {task_to_run.path_hash_key()} {task_to_run}')
                            else:
                                logger.info(f'Rescanning: {task_to_run.inputs["filepath"]}')
                            task_enqueued = self.enqueue_task(task_to_run, force=force)
                            if not task_enqueued:
                                self.task_complete(task_to_run)
                            task_to_run = None
                        else:
                            completed_task = self.executor.get_completed_task()
                            self.task_complete(completed_task)
                while not self.executor.has_finished():
                    logger.debug('waiting for remaining tasks')
                    task = self.executor.get_completed_task()
                    self.task_complete(task)
            finally:
                self.executor.finish()

    @check_finalized(True)
    def run_one(self, *, force=False, display_func=None):
        task = next(self.get_next_pending())
        self.run(requested_tasks=[task])

    @check_finalized(True)
    def print_status(self):
        print(f'{self.name}')
        self.statuses.print_status()
