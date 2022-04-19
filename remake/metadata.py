import os
import datetime as dt
import json
from hashlib import sha1
from logging import getLogger
from pathlib import Path
from time import sleep

from remake.flags import RemakeOn
from remake.util import sha1sum


logger = getLogger(__name__)

METADATA_VERSION = 'metadata_v5'
JSON_READ_ATTEMPTS = 3


def flush_json_write(obj, path):
    logger.debug(f'    write json to: {path}')
    with path.open('w') as fp:
        json.dump(obj, fp, indent=2)
        fp.write('\n')
        fp.flush()
        os.fsync(fp)


def try_json_read(path):
    attempts = 0

    while True:
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError as jde:
            attempts += 1
            logger.error(jde)
            logger.error(path)
            logger.error(f'attempts: {attempts}')
            if attempts == JSON_READ_ATTEMPTS:
                raise
        sleep(attempts * 5)


class NoMetadata(Exception):
    pass


class MetadataManager:
    """Creates and stores maps of PathMetadata and TaskMetadata"""
    # Needed because it keeps track of all PathMetadata objs, and stops there being duplicate ones for inputs.
    def __init__(self, task_control_name, dotremake_dir, content_checks):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.content_checks = content_checks
        self.path_metadata_map = {}
        self.task_metadata_map = {}

    def create_task_metadata(self, task):
        task_inputs_metadata_map = {}
        task_outputs_metadata_map = {}
        for input_path, special_input_path in zip(task.inputs.values(), task.special_inputs.values()):
            if input_path not in self.path_metadata_map:
                input_md = self._create_path_metadata(input_path, special_input_path)
            else:
                input_md = self.path_metadata_map[input_path]
            task_inputs_metadata_map[input_path] = input_md

        for output_path, special_output_path in zip(task.outputs.values(), task.special_outputs.values()):
            if output_path not in self.path_metadata_map:
                output_md = self._create_path_metadata(output_path, special_output_path)
            else:
                output_md = self.path_metadata_map[output_path]
            task_outputs_metadata_map[output_path] = output_md
        task_md = TaskMetadata(self.task_control_name, self.dotremake_dir,
                               task, task_inputs_metadata_map, task_outputs_metadata_map,
                               self.content_checks)
        self.task_metadata_map[task] = task_md
        return task_md

    def check_task_status(self, task):
        changed_paths = []
        # Issue #34: only hits PathMetadata._load_metadata once now per path.
        requires_rerun = RemakeOn.NOT_NEEDED
        task_md = self.task_metadata_map[task]
        for path in task.inputs.values():
            if not path.exists():
                task_md.rerun_reasons.append(('input_path_does_not_exist', path))
                requires_rerun |= RemakeOn.MISSING_INPUT
                continue
            if self.content_checks:
                path_md = self.path_metadata_map[path]
                if path_md.compare_path_with_previous():
                    task_md.rerun_reasons.append(('input_path_metadata_has_changed', path))
                    requires_rerun |= RemakeOn.INPUTS_CHANGED
                    changed_paths.append(path)

        task_md.generate_metadata()
        requires_rerun = task_md.task_requires_rerun()
        return changed_paths, requires_rerun

    def _create_path_metadata(self, path, special_input_path):
        assert path not in self.path_metadata_map, f'path already tracked: {path}'
        path_md = PathMetadata(self.task_control_name, self.dotremake_dir, path, special_input_path)
        self.path_metadata_map[path] = path_md
        return path_md


class TaskMetadata:
    def __init__(self, task_control_name, dotremake_dir, task,
                 inputs_metadata_map, outputs_metadata_map, content_checks):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.task = task
        self.inputs_metadata_map = inputs_metadata_map
        self.outputs_metadata_map = outputs_metadata_map
        self.content_checks = content_checks

        self.task_path_hash_key = self.task.path_hash_key()

        self.metadata = {}
        self.new_metadata = {'task_control_name': task_control_name}
        self.requires_rerun = True
        self.rerun_reasons = []
        self.task_metadata_dir_path = None
        self.log_path = None

        self.task_metadata_dir = self.metadata_dir / 'task_metadata'
        self.task_metadata_dir_path = self.task_metadata_dir / self.task_path_hash_key[:2] / self.task_path_hash_key[2:]

        self.task_metadata_path = self.task_metadata_dir_path / 'task.metadata'

        self.task_status_dir = (self.metadata_dir / self.task_control_name / 'task_status' /
                                self.task_path_hash_key[:2] / self.task_path_hash_key[2:])

        self.task_status_path = self.task_status_dir / 'task.status'
        self.log_path = self.task_status_dir / 'task.log'

    def update_status(self, status):
        self.task_status_path.parent.mkdir(parents=True, exist_ok=True)
        with self.task_status_path.open('a') as fp:
            fp.write(f'{dt.datetime.now()};{status}\n')

    def _load_metadata(self):
        if self.task_metadata_path.exists():
            logger.debug(f'    reading task metadata: {self.task}')
            self.metadata = try_json_read(self.task_metadata_path)
            logger.debug(f'    read task metadata: {self.task}')
        else:
            raise NoMetadata(f'No metadata for task: {self.task}')

    def read_log(self):
        print(self.log_path.read_text())

    def generate_metadata(self):
        logger.debug(f'    generate metadata for {self.task}')

        task_source_sha1hex, task_bytecode_sha1hex, task_depends_on_sha1hex = self._task_sha1hex()
        if self.content_checks:
            task_content_sha1hex = self._content_sha1hex()
            self.new_metadata['task_content_sha1hex'] = task_content_sha1hex
        self.new_metadata['task_source_sha1hex'] = task_source_sha1hex
        self.new_metadata['task_bytecode_sha1hex'] = task_bytecode_sha1hex
        self.new_metadata['task_depends_on_sha1hex'] = task_depends_on_sha1hex

    def _task_sha1hex(self):
        if hasattr(self.task, 'func_source'):
            task_hash_data = [self.task.func_source]
        else:
            task_hash_data = []
        task_args_data = []
        task_source_sha1hex = sha1(''.join(task_hash_data + task_args_data).encode()).hexdigest()

        if hasattr(self.task, 'func_bytecode'):
            task_hash_data = [str(self.task.func_bytecode)]
        else:
            task_hash_data = []
        task_bytecode_sha1hex = sha1(''.join(task_hash_data + task_args_data).encode()).hexdigest()

        task_hash_data = []
        if hasattr(self.task, 'depends_on_sources'):
            for depend_on_source in self.task.depends_on_sources:
                task_hash_data.append(depend_on_source)
        task_depends_on_sha1hex = sha1(''.join(task_hash_data).encode()).hexdigest()

        return task_source_sha1hex, task_bytecode_sha1hex, task_depends_on_sha1hex

    def _content_sha1hex(self):
        content_hash_data = []
        for path in self.task.inputs.values():
            assert path.is_absolute()
            if not path.exists():
                logger.debug(f'    no path exists: {path}')
                return ''
            input_path_md = self.inputs_metadata_map[path]
            if 'sha1hex' not in input_path_md.metadata:
                return None
            content_hash_data.append(input_path_md.metadata['sha1hex'])
            # Note1, (see below) you CANNOT put things like task_source_sha1hex in here.
            # Why not? Because if you do, ANY change to source, even one that does not affect
            # the output will cause subsequent tasks to run.

        task_content_sha1hex = sha1(''.join(content_hash_data).encode()).hexdigest()
        return task_content_sha1hex

    def task_requires_rerun(self):
        assert self.new_metadata

        self.requires_rerun = RemakeOn.NOT_NEEDED
        self.rerun_reasons = []
        try:
            self._load_metadata()
        except NoMetadata:
            self.rerun_reasons.append(('task_has_not_been_run', None))
            self.requires_rerun |= RemakeOn.NO_TASK_METADATA

        logger.debug('    stat all files')
        earliest_output_path_mtime = float('inf')
        for output in self.task.outputs.values():
            if not output.exists():
                self.rerun_reasons.append(('output_path_does_not_exist', output))
                self.requires_rerun |= RemakeOn.MISSING_OUTPUT
                break
            earliest_output_path_mtime = min(earliest_output_path_mtime,
                                             output.lstat().st_mtime)
        if not self.requires_rerun:
            latest_input_path_mtime = 0
            for input_path in self.task.inputs.values():
                if not input_path.exists():
                    self.rerun_reasons.append(('input_path_does_not_exist', input_path))
                    self.requires_rerun |= RemakeOn.MISSING_INPUT
                    break

                latest_input_path_mtime = max(latest_input_path_mtime,
                                              input_path.lstat().st_mtime)
            if latest_input_path_mtime > earliest_output_path_mtime:
                self.requires_rerun |= RemakeOn.OLDER_OUTPUT
                self.rerun_reasons.append(('output_is_older_than_input', None))
        logger.debug('    statted all files')

        if not (self.requires_rerun & RemakeOn.NO_TASK_METADATA):
            if self.new_metadata['task_source_sha1hex'] != self.metadata['task_source_sha1hex']:
                self.requires_rerun |= RemakeOn.TASK_SOURCE_CHANGED
                self.rerun_reasons.append(('task_source_sha1hex_different', None))
            if self.new_metadata['task_bytecode_sha1hex'] != self.metadata['task_bytecode_sha1hex']:
                self.requires_rerun |= RemakeOn.TASK_BYTECODE_CHANGED
                self.rerun_reasons.append(('task_bytecode_sha1hex_different', None))
            if self.new_metadata['task_depends_on_sha1hex'] != self.metadata['task_depends_on_sha1hex']:
                self.requires_rerun |= RemakeOn.DEPENDS_SOURCE_CHANGED
                self.rerun_reasons.append(('task_depends_on_sha1hex_different', None))
            if self.content_checks:
                if self.new_metadata['task_content_sha1hex'] != self.metadata['task_content_sha1hex']:
                    self.requires_rerun |= RemakeOn.INPUTS_CHANGED
                    self.rerun_reasons.append(('task_content_sha1hex_different', None))

        logger.debug(f'    task requires rerun {self.requires_rerun}: {self.task}')
        return self.requires_rerun

    def write_task_metadata(self):
        logger.debug(f'    write task metadata {self.task_path_hash_key} to {self.task_metadata_path}')

        self.task_metadata_dir_path.mkdir(parents=True, exist_ok=True)
        # Minimize the number of writes to file.
        if hasattr(self.task, 'func_source') and hasattr(self.task, 'func_bytecode'):
            self.new_metadata['func_source'] = self.task.func_source
            self.new_metadata['func_bytecode'] = str(self.task.func_bytecode)

        flush_json_write(self.new_metadata, self.task_metadata_path)

        for output_path_md in self.outputs_metadata_map.values():
            if not output_path_md.path.exists():
                continue
            metadata_has_changed = output_path_md.compare_path_with_previous()
            if metadata_has_changed:
                output_path_md.write_new_metadata()


class PathMetadata:
    def __init__(self, task_control_name, dotremake_dir, path, special_input_path):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.path = path
        if special_input_path:
            self.special_input_path = special_input_path
        else:
            self.special_input_path = Path(*path.parts[1:])
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.file_metadata_dir = self.metadata_dir / 'file_metadata'

        if special_input_path.is_absolute():
            self.metadata_path = self.file_metadata_dir.joinpath(*(special_input_path.parent.parts[1:] +
                                                                   (f'{path.name}.metadata',)))
        else:
            self.metadata_path = self.file_metadata_dir.joinpath(*(special_input_path.parent.parts +
                                                                   (f'{path.name}.metadata',)))

        self.task_metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                                    (f'{path.name}.created_by.task',)))

        self.metadata = {}
        self.new_metadata = {'task_control_name': task_control_name}
        self.task_metadata = {}

        self.changes = []
        self.metadata_has_changed = False
        self.need_write = False

    def _load_metadata(self):
        if self.metadata_path.exists():
            logger.debug(f'    reading path metadata: {self.path}')
            self.metadata = try_json_read(self.metadata_path)
        else:
            raise NoMetadata(f'No metadata for {self.path}')

    def compare_path_with_previous(self):
        path = self.path
        logger.debug(f'    comparing path with previous: {path}')

        if self.metadata_path.exists():
            self._load_metadata()

        # N.B. lstat dereferences symlinks.
        logger.debug(f'    stat path: {path}')
        stat = path.lstat()
        self.new_metadata.update({'st_size': stat.st_size, 'st_mtime': stat.st_mtime})

        metadata_has_changed = False
        if self.metadata:
            if self.new_metadata['st_size'] != self.metadata['st_size']:
                metadata_has_changed = True
                self.changes.append('st_size_changed')
            if self.new_metadata['st_mtime'] != self.metadata['st_mtime']:
                metadata_has_changed = True
                self.changes.append('st_mtime_changed')
        else:
            metadata_has_changed = True

        return metadata_has_changed

    def gen_sha1hex(self):
        self.new_metadata['sha1hex'] = sha1sum(self.path)

    def write_new_metadata(self):
        if 'sha1hex' not in self.new_metadata:
            self.new_metadata['sha1hex'] = sha1sum(self.path)
        # Note1, (see above) you CANNOT put things like task_source_sha1hex in here.
        # Why not? Because if you do, ANY change to source, even one that does not affect
        # the output will cause subsequent tasks to run.
        # You cannot even put task_content_sha1hex in here. The reason being that if you do,
        # a task will have a different value for it if any of its inputs changed. If the task then runs
        # it will definitely set off a chain of runs in its downstream tasks, even though it may not be needed.
        # This is probably a bit of an edge case.
        logger.debug(f'    write new path metadata to {self.metadata_path}')
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write(self.new_metadata, self.metadata_path)
