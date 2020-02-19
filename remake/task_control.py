from collections import defaultdict
import json
from hashlib import sha1
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)

try:
    import networkx as nx
except NameError:
    nx = None


if nx:
    def tasks_as_networkx_graph(task_ctrl):
        assert task_ctrl.finalized
        G = nx.DiGraph()
        for task in task_ctrl.tasks:
            G.add_node(task)

            for prev_task in task_ctrl.prev_tasks[task]:
                G.add_edge(prev_task, task)
        return G

    def files_as_networkx_graph(task_ctrl):
        assert task_ctrl.finalized
        G = nx.DiGraph()
        for task in task_ctrl.tasks:
            for i in task.inputs:
                for o in task.outputs:
                    G.add_edge(i, o)
        return G


def _compare_write_metadata(file_metadata_dir, path):
    metadata_path = file_metadata_dir.joinpath(*path.parts[1:])
    existing_metadata = None
    if metadata_path.exists():
        existing_metadata = json.loads(metadata_path.read_text())
    stat = path.stat()
    metadata = {'st_size': stat.st_size,
                'st_mtime': stat.st_mtime}
    need_write = False
    created = False
    has_changed = False

    if existing_metadata:
        if not all([metadata[k] == existing_metadata[k] for k in ['st_size', 'st_mtime']]):
            need_write = True
            # Only recalc sha1hex if size or last modified time have changed.
            sha1hex = sha1(path.read_bytes()).hexdigest()
            metadata['sha1hex'] = sha1hex
            if sha1hex != existing_metadata['sha1hex']:
                logger.debug(f'{path} has changed!')
                has_changed = True
            else:
                logger.debug(f'{path} properties has changed but contents the same')
        else:
            metadata['sha1hex'] = existing_metadata['sha1hex']
    else:
        created = True
        metadata['sha1hex'] = sha1(path.read_bytes()).hexdigest()
        need_write = True

    if need_write:
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, indent=2) + '\n')

    return created, has_changed, metadata['sha1hex']


def calc_task_sha1hex(file_metadata_dir, task):
    task_hash_data = []
    for path in task.inputs:
        assert path.is_absolute()
        if not path.exists():
            return None, None
        created, has_changed, sha1hex = _compare_write_metadata(file_metadata_dir, path)
        task_hash_data.append(sha1hex)

    task_func_source = task.func_source
    task_hash_data.append(task_func_source)
    if task.func_args:
        task_hash_data.append(str(''.join(task.func_args)))
    if task.func_kwargs:
        task_hash_data.append(str(''.join([f'{k}{v}' for k, v in task.func_kwargs.items()])))
    task_sha1hex = sha1(''.join(task_hash_data).encode()).hexdigest()
    return task_sha1hex, task_func_source


def task_requires_rerun_based_on_contents(file_metadata_dir, task, task_sha1hex, overwrite_task_metadata=False):
    requires_rerun = False
    for path in task.outputs:
        if not path.exists():
            requires_rerun = True
            break
        assert path.is_absolute()
        output_task_metadata_path = file_metadata_dir.joinpath(*(path.parent.parts[1:] + (f'{path.name}.task',)))
        output_task_metadata = {'task_sha1hex': task_sha1hex}
        if output_task_metadata_path.exists():
            existing_output_task_metadata = json.loads(output_task_metadata_path.read_text())
            if output_task_metadata['task_sha1hex'] != existing_output_task_metadata['task_sha1hex']:
                requires_rerun = True
            if overwrite_task_metadata:
                output_task_metadata_path.write_text(json.dumps(output_task_metadata, indent=2) + '\n')
        else:
            output_task_metadata_path.write_text(json.dumps(output_task_metadata, indent=2) + '\n')
    return requires_rerun


def compare_task_with_previous_runs(file_metadata_dir, task_metadata_dir,
                                    task, overwrite_task_metadata=False):
    task_sha1hex, task_source = calc_task_sha1hex(file_metadata_dir, task)

    if not task_sha1hex:
        return True, None
    task_metadata_path = task_metadata_dir / task_sha1hex
    if not task_metadata_path.exists():
        task_metadata_path.write_text(task_source)

    requires_rerun = task_requires_rerun_based_on_contents(file_metadata_dir, task, task_sha1hex,
                                                           overwrite_task_metadata)
    return requires_rerun, task_sha1hex


# noinspection PyAttributeOutsideInit
class TaskControl:
    def __init__(self, enable_file_task_content_checks=False):
        self.enable_file_task_content_checks = enable_file_task_content_checks
        self.extra_checks = True
        self.tasks = []

        # Get added to as new tasks are added.
        self.output_task_map = {}
        self.input_task_map = defaultdict(list)
        self.task_from_hexdigest = {}

        self.reset()

        if self.enable_file_task_content_checks:
            self.file_metadata_dir = Path('.remake/file_metadata')
            self.task_metadata_dir = Path('.remake/task_metaadata')
            self.task_metadata_dir.mkdir(parents=True, exist_ok=True)

    def reset(self):
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

        self._dag_built = False
        return self

    def add(self, task):
        if self.finalized:
            raise Exception(f'TaskControl already finalized')

        for output in task.outputs:
            if output in self.output_task_map:
                raise Exception(f'Trying to add {output} twice')
        hexdigest = task.hexdigest()
        if hexdigest in self.task_from_hexdigest:
            raise Exception(f'Trying to add {task} twice')
        self.task_from_hexdigest[hexdigest] = task

        self.tasks.append(task)
        for input_path in task.inputs:
            self.input_task_map[input_path].append(task)
        for output in task.outputs:
            self.output_task_map[output] = task

        return task

    def _topogological_tasks(self):
        assert self._dag_built

        curr_tasks = set(self.input_tasks)
        all_tasks = set()
        while True:
            next_tasks = set()
            for curr_task in curr_tasks:
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
            if not next_tasks:
                break
            curr_tasks = next_tasks

    def finalize(self):
        if self.finalized:
            raise Exception(f'TaskControl already finalized')

        logger.debug('building task DAG')
        # Work out whether it is possible to create a run schedule and find initial tasks.
        # Fill in self.prev_tasks and self.next_tasks; these hold the information about the
        # task DAG.
        for task in self.tasks:
            is_input_task = True
            for input_path in task.inputs:
                if input_path in self.output_task_map:
                    is_input_task = False
                    # Every output is created by only one task.
                    input_task = self.output_task_map[input_path]
                    if input_task not in self.prev_tasks[task]:
                        self.prev_tasks[task].append(input_task)
                else:
                    # input_path is not going to be created by any tasks; it might still exist though:
                    if not input_path.exists():
                        raise Exception(f'No input file {input_path} exists or will be created for {task}')
                    self.input_paths.add(input_path)
            if is_input_task:
                self.input_tasks.add(task)

            for output_path in task.outputs:
                if output_path in self.input_task_map:
                    # Each input can be used by any number of tasks.
                    output_tasks = self.input_task_map[output_path]
                    for output_task in output_tasks:
                        if output_task not in self.next_tasks[task]:
                            self.next_tasks[task].extend(output_tasks)

        self._dag_built = True

        # Can now perform a topological sort.
        self.sorted_tasks = list(self._topogological_tasks())
        if self.extra_checks:
            assert len(self.sorted_tasks) == len(self.tasks)
            assert set(self.sorted_tasks) == set(self.tasks)

        # import ipdb; ipdb.set_trace()
        # Assign each task to one of three groups:
        # completed: task has been run and does not need to be rerun.
        # pending: task has been run and needs to be rerun.
        # remaining: task either needs to be rerun, or has previous tasks that need to be rerun.
        for task in self.sorted_tasks:
            task_state = 'completed'
            if self.enable_file_task_content_checks:
                requires_rerun = compare_task_with_previous_runs(self.file_metadata_dir,
                                                                 self.task_metadata_dir,
                                                                 task)[0]
            else:
                requires_rerun = task.requires_rerun()

            if task.can_run() and requires_rerun:
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

            logger.debug(f'task status: {task_state} - {task}')
            if task_state == 'completed':
                self.completed_tasks.append(task)
            elif task_state == 'pending':
                self.pending_tasks.append(task)
            elif task_state == 'remaining':
                self.remaining_tasks.add(task)

        if self.extra_checks:
            all_tasks_assigned = (set(self.completed_tasks) | set(self.pending_tasks) | set(self.remaining_tasks) ==
                                  set(self.tasks) and
                                  len(self.completed_tasks) + len(self.pending_tasks) + len(self.remaining_tasks) ==
                                  len(self.tasks))
            assert all_tasks_assigned, 'All tasks not assigned.'

        self.output_paths = set(self.output_task_map.keys()) - self.input_paths

        self.finalized = True
        return self

    def get_next_pending(self):
        while self.pending_tasks or self.running_tasks:
            if not self.pending_tasks:
                yield None
            else:
                task = self.pending_tasks.pop(0)
                self.running_tasks.append(task)
                yield task

    def task_complete(self, task):
        assert task in self.running_tasks, 'task not being run'
        assert task.complete(), 'task not complete'
        logger.debug(f'add completed task: {task}')
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
            if requires_rerun and next_task.requires_rerun():
                logger.debug(f'adding new pending task: {next_task}')
                self.pending_tasks.append(next_task)
                self.remaining_tasks.remove(next_task)

    def _run_task(self, task, force=False):
        if task is None:
            raise Exception('No task to run')
        task_run_index = len(self.completed_tasks) + len(self.running_tasks)
        print(f'{task_run_index}/{len(self.tasks)}: {repr(task)}')
        task_sha1hex = None
        if self.enable_file_task_content_checks:
            logger.debug('performing task file contents checks')
            requires_rerun, task_sha1hex = compare_task_with_previous_runs(self.file_metadata_dir,
                                                                           self.task_metadata_dir,
                                                                           task, overwrite_task_metadata=False)
            force = force or requires_rerun
        logger.debug(f'running task (force={force}) {task}')
        task.run(force=force)

        if self.enable_file_task_content_checks:
            logger.debug('performing task file contents checks and writing data')
            task_requires_rerun_based_on_contents(self.file_metadata_dir, task, task_sha1hex, True)
            if self.extra_checks:
                requires_rerun = task_requires_rerun_based_on_contents(self.file_metadata_dir, task, task_sha1hex)
                assert not requires_rerun
        self.task_complete(task)

    def run(self, force=False):
        if not self.finalized:
            raise Exception(f'TaskControl not finalized')

        for task in self.get_next_pending():
            self._run_task(task, force)

    def run_one(self, force=False):
        if not self.finalized:
            raise Exception(f'TaskControl not finalized')

        task = next(self.get_next_pending())
        self._run_task(task, force)

    def rescan_metadata_completed_tasks(self):
        if not self.finalized:
            raise Exception(f'TaskControl not finalized')

        for task in self.completed_tasks:
            requires_rerun = compare_task_with_previous_runs(self.file_metadata_dir, self.task_metadata_dir,
                                                             task, overwrite_task_metadata=False)[0]
            if requires_rerun:
                print(f'{task} requires rerun')

    def print_status(self):
        print(f'completed: {len(self.completed_tasks)}')
        print(f'pending  : {len(self.pending_tasks)}')
        print(f'remaining: {len(self.remaining_tasks)}')
        print(f'all      : {len(self.tasks)}')

