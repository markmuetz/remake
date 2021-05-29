from collections import defaultdict, Counter
import functools
from logging import getLogger
import math
from pathlib import Path
from typing import List

import networkx as nx

from remake.task import RescanFileTask
from remake.metadata import MetadataManager
from remake.flags import RemakeOn
from remake.executor import SingleprocExecutor, MultiprocExecutor, SlurmExecutor
from remake.remake_exceptions import CyclicDependency

logger = getLogger(__name__)


class TaskStatuses:
    def __init__(self):
        self._all_tasks = set()
        self._rescan_tasks = []
        self._cannot_run_tasks = set()
        self._completed_tasks = set()
        self._pending_tasks = set()
        self._ordered_pending_tasks = []
        self._running_tasks = set()
        self._remaining_tasks = set()
        self._task_status = {}

    def reset(self):
        self.__init__()

    def add_task(self, task, status):
        self._all_tasks.add(task)
        if isinstance(task, RescanFileTask):
            assert status == 'pending'
            self._rescan_tasks.append(task)
        else:
            getattr(self, f'_{status}_tasks').add(task)
            if status == 'pending':
                self._ordered_pending_tasks.append(task)
        self._task_status[task] = status
        task.update_status(status)

    def get_next_pending(self):
        task = self._ordered_pending_tasks[0]
        self.update_task(task, 'pending', 'running')
        return task

    def update_task(self, task, old_status, new_status):
        if isinstance(task, RescanFileTask):
            assert self._task_status[task] in old_status
        else:
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
        task.update_status(new_status)

    def task_status(self, task):
        return self._task_status[task]

    @property
    def all_tasks(self):
        return self._all_tasks

    @property
    def rescan_tasks(self):
        return self._rescan_tasks

    @property
    def cannot_run_tasks(self):
        return self._cannot_run_tasks

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
        print(f'  cannot run: {len(self.cannot_run_tasks)}')
        print(f'  completed : {len(self.completed_tasks)}')
        print(f'  rescan    : {len(self.rescan_tasks)}')
        print(f'  pending   : {len(self.pending_tasks)}')
        print(f'  running   : {len(self.running_tasks)}')
        print(f'  remaining : {len(self.remaining_tasks)}')
        print(f'  all       : {len(self._task_status)}')


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
    def __init__(self, filename: str, config: dict = None, dependencies: List['TaskControl'] = None, *,
                 remake_on: RemakeOn = RemakeOn.ANY_STANDARD_CHANGE,
                 dotremake_dir='.remake',
                 print_reasons=False):
        self.check_path_metadata = True
        self.filename = filename
        if not config:
            config = {}
        self.config = config
        self.dependencies = dependencies
        self.path = Path(filename).absolute()
        self.name = self.path.stem
        self.remake_on = remake_on
        self.print_reasons = print_reasons
        self.extra_checks = True
        self.tasks = []
        self.display_func = None

        # Get added to as new tasks are added.
        self.dotremake_dir = Path(dotremake_dir)
        self.dotremake_dir.mkdir(parents=True, exist_ok=True)
        self.set_executor('singleproc')

        self.reset()

    def reset(self):
        self.metadata_manager = MetadataManager(self.name, self.dotremake_dir)
        self.finalized = False

        self.output_task_map = {}
        self.input_task_map = defaultdict(list)
        self.task_from_path_hash_key = {}

        # Generated by self.finalize()
        self.input_only_paths = set()
        self.output_paths = set()
        self.input_tasks = set()

        self.task_dag = nx.DiGraph()
        # Not necessarily a DAG.
        # Can have one rule that defines tasks that link back and forth to tasks in another rule.
        # tasks must form DAG; rules don't have to.
        self.rule_graph = nx.DiGraph()
        self.sorted_tasks = {}
        self.rescan_paths = set()

        self.statuses = TaskStatuses()
        self.subset_statuses = None
        self.forced_next_pending_tasks = []
        self.forced_run_tasks = set()

        self.tasks_at_level = {}

        self._dag_built = False
        tasks = [t for t in self.tasks]
        self.tasks = []
        for task in tasks:
            self.add(task)
        return self

    def set_executor(self, executor):
        if executor == 'singleproc':
            self.executor = SingleprocExecutor(self)
        elif executor == 'slurm':
            slurm_config = self.config.get('slurm', {})
            self.executor = SlurmExecutor(self, slurm_config)
        else:
            logger.warning('multiproc executor is still experimental')
            # r = input('PRESS y to continue: ')
            # if r != 'y':
            #     raise Exception('Fix MultiprocExecutor')
            self.executor = MultiprocExecutor(self)

    @property
    def rescan_tasks(self):
        return self.statuses.rescan_tasks

    @property
    def cannot_run_tasks(self):
        return self.statuses.cannot_run_tasks

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
        task.check_path_metadata = self.check_path_metadata

        return task

    def _topogological_tasks(self):
        assert self._dag_built

        count = 0
        level = 0
        curr_tasks = set(self.input_tasks)
        visited = Counter()
        all_tasks = set()
        while curr_tasks:
            self.tasks_at_level[level] = sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0])
            next_tasks = set()
            for curr_task in sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0]):
                if curr_task in visited and visited[curr_task] > len(list(self.task_dag.predecessors(curr_task))):
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

                    raise CyclicDependency(f'cycle detected in DAG:\n  {curr_task} produces input for\n  >' +
                                           '\n  >'.join([str(t) for t in input_tasks]) + '\n',
                                           [curr_task] + input_tasks)
                visited[curr_task] += 1
                can_yield = True
                # for prev_task in self.prev_tasks[curr_task]:
                for prev_task in self.task_dag.predecessors(curr_task):
                    if prev_task not in all_tasks:
                        can_yield = False
                        break
                if can_yield and curr_task not in all_tasks:
                    yield curr_task, count
                    count += 1
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

        if self.check_path_metadata:
            # TODO: Possibly switch to one rescan task per task (instead of one per task input).
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
        if not self.check_path_metadata and requires_rerun & RemakeOn.INPUTS_CHANGED:
            requires_rerun &= ~RemakeOn.INPUTS_CHANGED

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
        # N.B. this should only happen once per path.
        path = Path(path)
        if path in self.rescan_paths:
            return
        self.rescan_paths.add(path)
        path_md = self.metadata_manager.path_metadata_map[path]
        if path in self.input_task_map and path in self.output_task_map:
            pathtype = 'inout'
        elif path in self.input_task_map:
            pathtype = 'in'
        else:
            raise Exception('gen_rescan_task should never be called on output only path')
        rescan_task = RescanFileTask(self, path_md.path, path_md, pathtype)
        task_md = self.metadata_manager.create_task_metadata(rescan_task)
        if path.exists():
            rescan_task.add_metadata(task_md)

        # self.statuses.add_task(rescan_task, 'pending')
        if path.exists():
            self.statuses.add_task(rescan_task, 'pending')
        else:
            self.statuses.add_task(rescan_task, 'cannot_run')
        for next_task in self.input_task_map[path]:
            if next_task is not rescan_task:
                self.task_dag.add_edge(rescan_task, next_task)
        return rescan_task

    @check_finalized(False)
    def finalize(self):
        logger.info(f'=> {self.name} <=')
        if not self.tasks:
            raise Exception('No tasks have been added')

        if self.dependencies:
            for dep in self.dependencies:
                dep.finalize()
                if not dep.tasks == dep.completed_tasks:
                    raise Exception(f'Dependency task control {dep.name} is not complete')

        logger.info('Build task DAG')
        self.build_task_DAG()

#         missing_paths = [p for p in self.input_only_paths if not p.exists()]
#         if missing_paths:
#             for input_path in missing_paths:
#                 tasks = self.input_task_map[input_path]
#                 logger.error(f'No input file {input_path} exists or will be created (needed by {len(tasks)} tasks)')
#             raise Exception(f'Not all input paths exist: {len(missing_paths)} missing')
#
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
            all_tasks_assigned = (self.cannot_run_tasks | self.completed_tasks |
                                  self.pending_tasks | self.remaining_tasks ==
                                  set(self.tasks) and
                                  len(self.cannot_run_tasks) + len(self.completed_tasks) +
                                  len(self.pending_tasks) + len(self.remaining_tasks) ==
                                  len(self.tasks))
            assert all_tasks_assigned, 'All tasks not assigned.'

        self.output_paths = set(self.output_task_map.keys()) - self.input_only_paths

        self.finalized = True
        return self

    def _assign_tasks(self):
        # Assign each task to one of four groups:
        # cannot_run: not possible to run task (missing inputs).
        # completed: task has been run and does not need to be rerun.
        # pending: task has been run and needs to be rerun.
        # remaining: task either needs to be rerun, or has previous tasks that need to be rerun.
        # import ipdb; ipdb.set_trace()
        for task in self.sorted_tasks.keys():
            requires_rerun = self.task_requires_rerun(task)

            if task.can_run():
                status = 'completed'
                if task.force:
                    status = 'pending'
                else:
                    for prev_task in self.task_dag.predecessors(task):
                        if prev_task in self.pending_tasks or prev_task in self.remaining_tasks or isinstance(prev_task, RescanFileTask):
                            status = 'remaining'
                            break
                    if status != 'remaining' and requires_rerun & self.remake_on:
                        status = 'pending'
            else:
                status = 'remaining'
                # Reasons task can be cannot run:
                # 1: one of its prev tasks cannot be run.
                # 2: it has no previous tasks and thinks it cannot be run (task.can_run() == False).
                # 3: it has a file that does not exists as an input and that file is not an output of any other task.
                prev_tasks = list(self.task_dag.predecessors(task))
                if prev_tasks:
                    for prev_task in prev_tasks:
                        if prev_task in self.cannot_run_tasks:
                            status = 'cannot_run'
                            break
                else:
                    status = 'cannot_run'
                for input_path in task.inputs.values():
                    if input_path not in self.output_task_map and not input_path.exists():
                        status = 'cannot_run'
                        break

            logger.debug(f'  task status: {status} - {task}')
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
                    self.rule_graph.add_edge(input_task.__class__, task.__class__)
                    # if input_task not in self.task_dag.predecessors(task):
                    #    task.__class__.prev_rules.add(input_task.__class__)
                    #    input_task.__class__.next_rules.add(task.__class__)
                else:
                    self.input_only_paths.add(input_path)
            if is_input_task:
                self.input_tasks.add(task)

        self._dag_built = True
        logger.info('Perform topological sort')
        # Can now perform a topological sort.
        self.sorted_tasks = dict(self._topogological_tasks())

    def get_next_pending(self):
        while self.rescan_tasks or self.pending_tasks or self.remaining_tasks:
            if self.rescan_tasks:
                yield self.rescan_tasks.pop(0)
            elif not self.pending_tasks:
                yield None
            else:
                yield self.statuses.get_next_pending()

    def get_forced_next_pending(self):
        run_tasks = set()
        while self.forced_next_pending_tasks:
            task = self.forced_next_pending_tasks[0]
            pending = True
            for prev_task in self.task_dag.predecessors(task):
                if prev_task not in self.forced_run_tasks:
                    if prev_task in run_tasks:
                        pending = False
                        break
                    if prev_task.status != 'completed':
                        pending = False
                        break
            if pending:
                task = self.forced_next_pending_tasks.pop(0)
                run_tasks.add(task)
                yield task
            else:
                yield None

    def get_next_pending_from_subset(self):
        while (self.subset_statuses.rescan_tasks or
               self.subset_statuses.pending_tasks or
               self.subset_statuses.remaining_tasks):
            if self.subset_statuses.rescan_tasks:
                task = self.subset_statuses.rescan_tasks.pop(0)
                yield task
            elif not self.subset_statuses.pending_tasks:
                yield None
            else:
                task = self.subset_statuses.get_next_pending()
                self.statuses.update_task(task, 'pending', 'running')
                yield task

    def enqueue_task(self, task, force=False):
        if task is None:
            raise Exception('No task to enqueue')
        status = self.statuses.task_status(task)
        # self.update_task_status(task, status, 'running')

        requires_rerun = self.task_requires_rerun(task, print_reasons=self.print_reasons)
        if requires_rerun & self.remake_on or force:
            logger.debug(f'enqueue task (force={force}, requires_rerun={requires_rerun}): {task}')

            try:
                self.executor.enqueue_task(task)
            except Exception as e:
                logger.error(f'TaskControl: {self.name}')
                logger.error(e)
                task.task_md.update_status('ERROR')
                self.update_task_status(task, 'running', status)
                raise
            logger.debug(f'enqueued task: {task}')
            return True
        else:
            logger.debug(f'no longer requires enqueued: {task}')
            logger.info(f'  -> task run not needed')
            # TODO: at this point the DAG could be rescanned, and any downstream tasks could be marked as completed.
            return False

    def task_complete(self, task, force=False):
        assert task.complete(), f'{task} not complete'
        logger.debug(f'add completed task: {task}')
        # Task is not necessarily running if multiproc running?
        # TODO: investigate further. Run examples/ex1 with multiproc.
        self.update_task_status(task, task.status, 'completed')
        if force:
            self.forced_run_tasks.add(task)

        for next_task in self.task_dag.successors(task):
            if next_task in self.statuses.cannot_run_tasks:
                continue
            if next_task in self.completed_tasks:
                continue
            add_to_pending = True
            # Make sure all previous tasks have been run.
            for prev_task in self.task_dag.predecessors(next_task):
                if not prev_task.status == 'completed' and not prev_task.status == 'cannot_run':
                    add_to_pending = False
                    break
            if add_to_pending:
                logger.debug(f'adding new pending task: {next_task}')
                curr_status = self.statuses.task_status(next_task)
                self.update_task_status(next_task, curr_status, 'pending')

        if self.display_func:
            self.display_func(self)

    @check_finalized(True)
    def run_rescan_only(self):
        self.run_requested(self.rescan_tasks)

    @check_finalized(True)
    def run_requested(self, requested_tasks, force=False):
        if self.executor.handles_dependencies:
            with self.executor:
                # Work here is done, just enqueue all pending and remaining tasks:
                for task in requested_tasks:
                    self.executor.enqueue_task(task)
            return

        def sorter(task):
            if isinstance(task, RescanFileTask):
                # These should come first!
                return -1 * (len(self.rescan_tasks) - self.rescan_tasks.index(task))
            else:
                return self.sorted_tasks[task]

        self.subset_sorted_tasks = sorted(requested_tasks, key=sorter)

        if not force:
            self.subset_statuses = TaskStatuses()
            for task in self.subset_sorted_tasks:
                self.subset_statuses.add_task(task, self.statuses.task_status(task))
        try:
            self.run_all(force=force, use_subset=True)
        finally:
            self.subset_statuses = None
            self.subset_sorted_tasks = None

    def _forced_assign_tasks(self, use_subset=False):
        if use_subset:
            sorted_tasks = self.subset_sorted_tasks
        else:
            sorted_tasks = self.rescan_tasks + list(self.sorted_tasks.keys())
        for task in sorted_tasks:
            if task.status == 'cannot_run':
                break
            self.forced_next_pending_tasks.append(task)

    @check_finalized(True)
    def run_all(self, force=False, use_subset=False):
        if force:
            self.forced_next_pending_tasks = []
            self.forced_run_tasks = set()
            self._forced_assign_tasks(use_subset=use_subset)

        if self.executor.handles_dependencies:
            remaining_tasks = sorted(self.remaining_tasks, key=self.sorted_tasks.get)
            tasks = self.rescan_tasks + self.statuses.ordered_pending_tasks + remaining_tasks
            with self.executor:
                # Work here is done, just enqueue all pending and remaining tasks:
                for task in tasks:
                    # TODO: make work for force=True.
                    self.executor.enqueue_task(task)
            return
        with self.executor:
            if force:
                get_next_pending = self.get_forced_next_pending
            else:
                if use_subset:
                    get_next_pending = self.get_next_pending_from_subset
                else:
                    get_next_pending = self.get_next_pending
            run_tasks = set()

            try:
                for pending_task in get_next_pending():
                    if pending_task:
                        assert pending_task not in run_tasks, f'Cannot run task twice!: {pending_task}'
                    run_tasks.add(pending_task)

                    while True:
                        if pending_task:
                            # Got a pending task.
                            if self.executor.can_accept_task():
                                logger.debug(f'Executor can accept {pending_task}')
                                # Executor can accept tasks -- enqueue it.
                                self.log_task_info(self.sorted_tasks.get,
                                                   len(self.sorted_tasks),
                                                   pending_task)
                                task_enqueued = self.enqueue_task(pending_task, force=force)
                                if not task_enqueued:
                                    self.task_complete(pending_task, force=force)
                                # Go get a new pending task.
                                break
                            else:
                                logger.debug('Executor cannot accept')
                                # Executor cant accept tasks - wait for slot and rerun with pending_task
                                completed_task = self.executor.get_completed_task()
                                self.task_complete(completed_task, force=force)
                        else:
                            logger.debug('No pending tasks')
                            # No pending tasks because they are all enqueued.
                            completed_task = self.executor.get_completed_task()
                            self.task_complete(completed_task, force=force)
                            break

                while not self.executor.has_finished():
                    logger.debug('waiting for remaining tasks')
                    task = self.executor.get_completed_task()
                    self.task_complete(task, force=force)
            finally:
                self.forced_run_tasks = None

    def log_task_info(self, task_index, len_tasks, task):
        if not isinstance(task, RescanFileTask):
            num_digits = math.floor(math.log10(len_tasks)) + 1
            # N.B. double subs allowed in fstrings!
            logger.info(f'{task_index(task) + 1:>{num_digits}}/{len_tasks}: {task}')
        else:
            logger.info(f'Rescanning: {task.inputs["filepath"]}')

    @check_finalized(True)
    def print_status(self):
        print(f'{self.name}')
        self.statuses.print_status()

    def update_task_status(self, task, old_status, new_status):
        self.statuses.update_task(task, old_status, new_status)
        if self.subset_statuses and task in self.subset_statuses.all_tasks:
            self.subset_statuses.update_task(task, old_status, new_status)
