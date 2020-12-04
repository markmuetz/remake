import itertools
from collections import defaultdict, Mapping
import functools
from logging import getLogger
from pathlib import Path
from typing import List

from remake.task import Task
from remake.metadata import MetadataManager
from remake.flags import RemakeOn
from remake.util import fmtp

logger = getLogger(__name__)


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
        if self.remake_on & RemakeOn.ANY_METADATA_CHANGE:
            self.enable_file_task_content_checks = True
        self.print_reasons = print_reasons
        self.extra_checks = True
        self.tasks = []

        # Get added to as new tasks are added.
        self.output_task_map = {}
        self.input_task_map = defaultdict(list)
        self.task_from_path_hash_key = {}
        if self.enable_file_task_content_checks:
            self.dotremake_dir = Path(dotremake_dir)
            self.dotremake_dir.mkdir(parents=True, exist_ok=True)

        self.reset()

    def reset(self):
        if self.enable_file_task_content_checks:
            self.metadata_manager = MetadataManager(self.name, self.dotremake_dir)
        self.finalized = False

        # Generated by self.finalize()
        self.input_paths = set()
        self.output_paths = set()
        self.input_tasks = set()

        self.prev_tasks = defaultdict(list)
        self.next_tasks = defaultdict(list)

        self.sorted_tasks = []

        self.completed_tasks = []
        self.pending_tasks = []
        self.running_tasks = []
        self.remaining_tasks = set()

        self.tasks_at_level = {}

        self._dag_built = False
        return self

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

        return task

    def _topogological_tasks(self):
        assert self._dag_built

        level = 0
        curr_tasks = set(self.input_tasks)
        all_tasks = set()
        while curr_tasks:
            self.tasks_at_level[level] = sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0])
            next_tasks = set()
            for curr_task in sorted(curr_tasks, key=lambda t: list(t.outputs.values())[0]):
                can_yield = True
                for prev_task in self.prev_tasks[curr_task]:
                    if prev_task not in all_tasks:
                        can_yield = False
                        break
                if can_yield and curr_task not in all_tasks:
                    yield curr_task
                    all_tasks.add(curr_task)

                for next_task in self.next_tasks[curr_task]:
                    next_tasks.add(next_task)
            curr_tasks = next_tasks
            level += 1

    def task_requires_rerun(self, task, print_reasons=False):
        requires_rerun = RemakeOn.NOT_NEEDED
        if self.enable_file_task_content_checks:
            task_md = self.metadata_manager.task_metadata_map[task]
            logger.debug('performing task file contents checks')

            generated = task_md.generate_metadata()
            if generated:
                requires_rerun = task_md.task_requires_rerun()
            else:
                requires_rerun = RemakeOn.MISSING_INPUT

            if not requires_rerun & RemakeOn.MISSING_OUTPUT and \
                    task_md.task.remake_required and not task_md.task.remake_on & requires_rerun:
                logger.debug(f'{task_md.task.remake_on} not in {requires_rerun}')
                if print_reasons:
                    print(f'  --reason: not needed')
                return RemakeOn.NOT_NEEDED
            if requires_rerun:
                logger.debug(f'requires rerun: {requires_rerun}')
                for reason in task_md.rerun_reasons:
                    logger.debug(f'  {reason}')
                    if print_reasons:
                        print(f'  --reason: {reason}')
        else:
            if task.can_run():
                requires_rerun = task.requires_rerun()
        if print_reasons and not requires_rerun:
            print(f'  --reason: not needed')
        return requires_rerun

    @check_finalized(False)
    def finalize(self):
        if not self.tasks:
            raise Exception('No tasks have been added')

        if self.dependencies:
            for dep in self.dependencies:
                dep.finalize()
                if not dep.tasks == dep.completed_tasks:
                    raise Exception(f'Dependency task control {dep.name} is not complete')

        logger.debug('building task DAG')
        self.build_task_DAG()

        missing_paths = [p for p in self.input_paths if not p.exists()]
        if missing_paths:
            for input_path in missing_paths:
                tasks = self.input_task_map[input_path]
                logger.error(f'No input file {input_path} exists or will be created (needed by {len(tasks)} tasks)')
            raise Exception(f'Not all input paths exist: {len(missing_paths)} missing')

        if not self.input_tasks:
            raise Exception('No input tasks defined (do you have a circular dependency?)')

        logger.debug('perform topological sort')
        # Can now perform a topological sort.
        self.sorted_tasks = list(self._topogological_tasks())
        if self.extra_checks:
            logger.debug('performing extra checks on sorted tasks')
            assert len(self.sorted_tasks) == len(self.tasks)
            assert set(self.sorted_tasks) == set(self.tasks)

        if self.enable_file_task_content_checks:
            logger.debug('writing input path metadata')
            # Can now write all metadata for paths (size, mtime and sha1sum).
            for input_path in sorted(self.input_paths):
                # N.B. already created in self.build_task_DAG()
                input_md = self.metadata_manager.path_metadata_map[input_path]
                _, needs_write = input_md.compare_path_with_previous()
                if needs_write:
                    input_md.write_new_metadata()

        logger.debug('assigning tasks to groups')
        self._assign_tasks()

        if self.extra_checks:
            logger.debug('performing extra checks on groups')
            all_tasks_assigned = (set(self.completed_tasks) | set(self.pending_tasks) | set(self.remaining_tasks) ==
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
            task_state = 'completed'
            requires_rerun = self.task_requires_rerun(task)

            if task.can_run() and (requires_rerun & self.remake_on) or task.force:
                task_state = 'pending'
                for prev_task in self.prev_tasks[task]:
                    if prev_task in self.pending_tasks or prev_task in self.remaining_tasks:
                        task_state = 'remaining'
                        break
            else:
                for prev_task in self.prev_tasks[task]:
                    if prev_task in self.pending_tasks or prev_task in self.remaining_tasks:
                        task_state = 'remaining'
                        break

            logger.debug(f'  task status: {task_state} - {task.path_hash_key()}')
            if task_state == 'completed':
                self.completed_tasks.append(task)
            elif task_state == 'pending':
                self.pending_tasks.append(task)
            elif task_state == 'remaining':
                self.remaining_tasks.add(task)

    def build_task_DAG(self):
        if self._dag_built:
            logger.info('DAG already built')
            return

        # Work out whether it is possible to create a run schedule and find initial tasks.
        # Fill in self.prev_tasks and self.next_tasks; these hold the information about the
        # task DAG.
        for task in self.tasks:
            if self.enable_file_task_content_checks:
                self.metadata_manager.create_task_metadata(task)
            is_input_task = True
            for input_path in task.inputs.values():
                if input_path in self.output_task_map:
                    is_input_task = False
                    # Every output is created by only one task.
                    input_task = self.output_task_map[input_path]
                    if input_task not in self.prev_tasks[task]:
                        self.prev_tasks[task].append(input_task)
                        task.__class__.prev_rules.add(input_task.__class__)
                        input_task.__class__.next_rules.add(task.__class__)

                else:
                    self.input_paths.add(input_path)
            if is_input_task:
                self.input_tasks.add(task)

            for output_path in task.outputs.values():
                if output_path in self.input_task_map:
                    # Each input can be used by any number of tasks.
                    output_tasks = self.input_task_map[output_path]
                    for output_task in output_tasks:
                        if output_task not in self.next_tasks[task]:
                            self.next_tasks[task].extend(output_tasks)
                            task.__class__.next_rules.add(output_task.__class__)
                            output_task.__class__.prev_rules.add(task.__class__)
        self._dag_built = True

    def get_next_pending(self):
        while self.pending_tasks or self.running_tasks:
            if not self.pending_tasks:
                yield None
            else:
                task = self.pending_tasks.pop(0)
                yield task

    def task_complete(self, task):
        assert task in self.running_tasks, 'task not being run'
        assert task.complete(), 'task not complete'
        logger.debug(f'add completed task: {task.path_hash_key()}')
        self.running_tasks.remove(task)
        self.completed_tasks.append(task)

        for next_task in self.next_tasks[task]:
            requires_rerun = True
            # Make sure all previous tasks have been run.
            for prev_tasks in self.prev_tasks[next_task]:
                if prev_tasks not in self.completed_tasks:
                    requires_rerun = False
                    break
            # According to precalculated values next task requires rerun.
            # What does next task think?
            if not self.enable_file_task_content_checks:
                requires_rerun = requires_rerun or next_task.requires_rerun()
            if requires_rerun:
                logger.debug(f'adding new pending task: {next_task.path_hash_key()}')
                self.pending_tasks.append(next_task)
                if next_task in self.remaining_tasks:
                    # N.B. happens if run(force=True) is used.
                    self.remaining_tasks.remove(next_task)

    def _post_run_with_content_check(self, task_md):
        logger.debug('post run content checks')
        generated = task_md.generate_metadata()
        assert generated, f'Could not generate metadata for {task_md.task}'
        task_md.write_task_metadata()

        if self.extra_checks:
            logger.debug('post run content checks extra_checks')
            requires_rerun = task_md.task_requires_rerun()
            assert not requires_rerun

    def run_task(self, task, force=False):
        if task is None:
            raise Exception('No task to run')
        if self.enable_file_task_content_checks:
            task_md = self.metadata_manager.task_metadata_map[task]
            requires_rerun = self.task_requires_rerun(task, print_reasons=self.print_reasons)
            if force or task.force or requires_rerun & self.remake_on:
                logger.debug(f'running task (force={force}, requires_rerun={requires_rerun}): {repr(task)}')
                task_md.log_path.parent.mkdir(parents=True, exist_ok=True)
                # TODO: adding file logging is disabling other logging.
                # add_file_logging(task_md.log_path)
                task_md.update_status('RUNNING')
                try:
                    task.run(force=True)
                    task_md.update_status('COMPLETE')
                    print(f'  -> task complete')
                except Exception as e:
                    logger.error(f'TaskControl: {self.name}')
                    logger.error(e)
                    task_md.update_status('ERROR')
                    raise
                finally:
                    # remove_file_logging(task_md.log_path)
                    pass
                logger.debug(f'run task completed: {repr(task)}')
                self._post_run_with_content_check(task_md)
            else:
                logger.debug(f'no longer requires rerun: {repr(task)}')
                print(f'  -> task run not needed')
        else:
            logger.debug(f'running task (force={force}) {repr(task)}')
            task.run(force=force)

    @check_finalized(True)
    def run(self,  *, requested_tasks=None, force=False, display_func=None):
        if force:
            if requested_tasks:
                tasks = sorted(requested_tasks, key=lambda x: self.sorted_tasks.index(x))
            else:
                tasks = [t for t in self.sorted_tasks]

            for task in tasks:
                self.running_tasks.append(task)
                print(f'{self.sorted_tasks.index(task) + 1}/{len(self.tasks)}: {task.path_hash_key()} {task}')
                self.run_task(task, True)
                self.task_complete(task)

                if display_func:
                    display_func(self)
        else:
            if not requested_tasks:
                for task in self.get_next_pending():
                    self.running_tasks.append(task)
                    task_run_index = len(self.completed_tasks) + len(self.running_tasks)
                    print(f'{task_run_index}/{len(self.tasks)}: {task.path_hash_key()} {task}')
                    self.run_task(task, force)
                    self.task_complete(task)

                    if display_func:
                        display_func(self)
            else:
                sorted_requested_tasks = sorted(requested_tasks, key=lambda x: self.sorted_tasks.index(x))
                for task in sorted_requested_tasks:
                    if task in self.pending_tasks:
                        self.pending_tasks.remove(task)
                    elif task in self.completed_tasks:
                        self.completed_tasks.remove(task)
                    elif task in self.remaining_tasks:
                        self.remaining_tasks.remove(task)

                    self.running_tasks.append(task)
                    print(f'{sorted_requested_tasks.index(task) + 1}/{len(sorted_requested_tasks)}:'
                          f' {task.path_hash_key()} {task}')
                    self.run_task(task, force)
                    self.task_complete(task)

                    if display_func:
                        display_func(self)

    @check_finalized(True)
    def run_one(self,  *, force=False, display_func=None):
        task = next(self.get_next_pending())
        self.running_tasks.append(task)
        task_run_index = len(self.completed_tasks) + len(self.running_tasks)
        print(f'{task_run_index}/{len(self.tasks)}: {task.path_hash_key()} {task}')
        self.run_task(task, force)
        self.task_complete(task)

        if display_func:
            display_func(self)

    @check_finalized(True)
    def rescan_metadata(self):
        for task in self.tasks:
            task_md = self.metadata_manager.task_metadata_map[task]
            generated = task_md.generate_metadata()
            if generated:
                task_md.write_task_metadata()

    @check_finalized(True)
    def print_status(self):
        print(f'{self.name}')
        print(f'  completed: {len(self.completed_tasks)}')
        print(f'  pending  : {len(self.pending_tasks)}')
        print(f'  running  : {len(self.running_tasks)}')
        print(f'  remaining: {len(self.remaining_tasks)}')
        print(f'  all      : {len(self.tasks)}')
