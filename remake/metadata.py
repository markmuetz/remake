import os
import datetime as dt
import json
from hashlib import sha1
from logging import getLogger
from time import sleep

from remake.flags import RemakeOn
from remake.util import sha1sum


logger = getLogger(__name__)

METADATA_VERSION = 'metadata_v4'
JSON_READ_ATTEMPTS = 3


def flush_json_write(obj, path):
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
            logger.debug(path)
            logger.debug(f'attempts: {attempts}')
            if attempts == JSON_READ_ATTEMPTS:
                raise
        sleep(attempts * 5)


class NoMetadata(Exception):
    pass


class MetadataManager:
    """Creates and stores maps of PathMetadata and TaskMetadata"""
    # Needed because it keeps track of all PathMetadata objs, and stops there being duplicate ones for inputs.
    def __init__(self, task_control_name, dotremake_dir, *, full_tracking=False):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.full_tracking = full_tracking
        self.path_metadata_map = {}
        self.task_metadata_map = {}

    def create_task_metadata(self, task):
        task_inputs_metadata_map = {}
        task_outputs_metadata_map = {}
        for input_path in task.inputs:
            if input_path not in self.path_metadata_map:
                input_md = self.create_path_metadata(input_path)
            else:
                input_md = self.path_metadata_map[input_path]
            task_inputs_metadata_map[input_path] = input_md

        for output_path in task.outputs:
            if output_path not in self.path_metadata_map:
                output_md = self.create_path_metadata(output_path)
            else:
                output_md = self.path_metadata_map[output_path]
            task_outputs_metadata_map[output_path] = output_md
        task_md = TaskMetadata(self.task_control_name, self.dotremake_dir,
                               task, task_inputs_metadata_map, task_outputs_metadata_map,
                               full_tracking=self.full_tracking)
        self.task_metadata_map[task] = task_md
        return task_md

    def create_path_metadata(self, path):
        assert path not in self.path_metadata_map, f'path already tracked: {path}'
        path_md = PathMetadata(self.task_control_name, self.dotremake_dir, path)
        self.path_metadata_map[path] = path_md
        return path_md


class TaskMetadata:
    def __init__(self, task_control_name, dotremake_dir, task, inputs_metadata_map, outputs_metadata_map,
                 *, full_tracking=False):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.task = task
        self.inputs_metadata_map = inputs_metadata_map
        self.outputs_metadata_map = outputs_metadata_map

        self.full_tracking = full_tracking
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
        self.log_path = self.task_metadata_dir_path / 'task.log'

        self.task_status_path = (self.metadata_dir / 'task_status' / self.task_path_hash_key[:2] /
                                 (self.task_path_hash_key[2:] + '.status'))

    def update_status(self, status):
        self.task_status_path.parent.mkdir(parents=True, exist_ok=True)
        with self.task_status_path.open('a') as fp:
            fp.write(f'{dt.datetime.now()};{status}\n')

    def load_metadata(self):
        if self.task_metadata_path.exists():
            self.metadata = try_json_read(self.task_metadata_path)
        else:
            raise NoMetadata(f'No metadata for task: {self.task}')

    def read_log(self):
        print(self.log_path.read_text())

    def generate_metadata(self):
        logger.debug(f'generate metadata for {self.task_path_hash_key}')

        task_source_sha1hex, task_bytecode_sha1hex, task_depends_on_sha1hex = self._task_sha1hex()
        content_sha1hex = self._content_sha1hex()
        if content_sha1hex is None:
            logger.debug(f'no existing content')
            self.rerun_reasons.append(('no_existing_input_paths', None))
            return False

        self.new_metadata['task_source_sha1hex'] = task_source_sha1hex
        self.new_metadata['task_bytecode_sha1hex'] = task_bytecode_sha1hex
        self.new_metadata['task_depends_on_sha1hex'] = task_depends_on_sha1hex
        self.new_metadata['content_sha1hex'] = content_sha1hex
        return True

    def _task_sha1hex(self):
        task_hash_data = [self.task.func_source]
        task_args_data = []
        if self.task.func_args:
            task_args_data.append(str(self.task.func_args))
        if self.task.func_kwargs:
            task_args_data.append(str(self.task.func_kwargs))
        task_source_sha1hex = sha1(''.join(task_hash_data + task_args_data).encode()).hexdigest()

        task_hash_data = [str(self.task.func_bytecode)]
        task_bytecode_sha1hex = sha1(''.join(task_hash_data + task_args_data).encode()).hexdigest()

        task_hash_data = []
        for depend_on_source in self.task.depends_on_sources:
            task_hash_data.append(depend_on_source)
        task_depends_on_sha1hex = sha1(''.join(task_hash_data).encode()).hexdigest()

        return task_source_sha1hex, task_bytecode_sha1hex, task_depends_on_sha1hex

    def _content_sha1hex(self):
        content_hash_data = []
        for path in self.task.inputs:
            assert path.is_absolute()
            if not path.exists():
                logger.debug(f'no path exists: {path}')
                return None
            input_path_md = self.inputs_metadata_map[path]
            content_has_changed, needs_write = input_path_md.compare_path_with_previous()
            if needs_write:
                input_path_md.write_new_metadata()
            if content_has_changed:
                self.rerun_reasons.append(('content_has_changed', path))
            if 'sha1hex' not in input_path_md.new_metadata:
                return None
            sha1hex = input_path_md.new_metadata['sha1hex']
            content_hash_data.append(sha1hex)

        content_sha1hex = sha1(''.join(content_hash_data).encode()).hexdigest()
        return content_sha1hex

    def task_requires_rerun(self):
        assert self.new_metadata

        self.requires_rerun = RemakeOn.NOT_NEEDED
        self.rerun_reasons = []
        try:
            self.load_metadata()
        except NoMetadata:
            self.rerun_reasons.append(('task_has_not_been_run', None))
            self.requires_rerun |= RemakeOn.NO_TASK_METADATA

        for path in self.task.inputs:
            if not path.exists():
                self.rerun_reasons.append(('input_path_does_not_exist', path))
                self.requires_rerun |= RemakeOn.MISSING_INPUT
                break

        for path in self.task.outputs:
            if not path.exists():
                self.rerun_reasons.append(('output_path_does_not_exist', path))
                self.requires_rerun |= RemakeOn.MISSING_OUTPUT
                break

        if not (self.requires_rerun & RemakeOn.NO_TASK_METADATA):
            if self.new_metadata['task_source_sha1hex'] != self.metadata['task_source_sha1hex']:
                self.requires_rerun |= RemakeOn.TASK_SOURCE_CHANGED
                self.rerun_reasons.append('task_source_sha1hex_different')
            if self.new_metadata['task_bytecode_sha1hex'] != self.metadata['task_bytecode_sha1hex']:
                self.requires_rerun |= RemakeOn.TASK_BYTECODE_CHANGED
                self.rerun_reasons.append('task_bytecode_sha1hex_different')
            if self.new_metadata['task_depends_on_sha1hex'] != self.metadata['task_depends_on_sha1hex']:
                self.requires_rerun |= RemakeOn.DEPENDS_SOURCE_CHANGED
                self.rerun_reasons.append('task_depends_on_sha1hex_different')
            if self.new_metadata['content_sha1hex'] != self.metadata['content_sha1hex']:
                self.requires_rerun |= RemakeOn.INPUTS_CHANGED
                self.rerun_reasons.append('content_sha1hex_different')

        logger.debug(f'task requires rerun {self.requires_rerun}: {self.task_path_hash_key}')
        return self.requires_rerun

    def write_task_metadata(self):
        logger.debug(f'write task metadata {self.task_path_hash_key} to {self.task_metadata_path}')

        self.task_metadata_dir_path.mkdir(parents=True, exist_ok=True)
        # Minimize the number of writes to file.
        self.new_metadata['func_source'] = self.task.func_source
        self.new_metadata['func_bytecode'] = str(self.task.func_bytecode)

        flush_json_write(self.new_metadata, self.task_metadata_path)

        if self.full_tracking:
            # Not absolutely needed.
            inputs_outputs_path = self.task_metadata_dir_path / 'inputs_outputs'
            task_inputs = [str(p) for p in self.task.inputs]
            task_outputs = [str(p) for p in self.task.outputs]
            logger.debug(f'write inputs/outputs to {inputs_outputs_path}')
            flush_json_write({'inputs': task_inputs, 'outputs': task_outputs}, inputs_outputs_path)

            for input_path_md in self.inputs_metadata_map.values():
                input_path_md.write_new_used_by_task_metadata(self.task_path_hash_key)

        for output_path_md in self.outputs_metadata_map.values():
            if not output_path_md.path.exists():
                continue
            _, needs_write = output_path_md.compare_path_with_previous()
            if needs_write:
                output_path_md.write_new_metadata()
            if self.full_tracking:
                # Not absolutely needed.
                output_path_md.compare_task_with_previous(self.task_path_hash_key)


class PathMetadata:
    def __init__(self, task_control_name, dotremake_dir, path):
        self.task_control_name = task_control_name
        self.dotremake_dir = dotremake_dir
        self.path = path
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.file_metadata_dir = self.metadata_dir / 'file_metadata'

        self.metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                               (f'{path.name}.metadata',)))
        self.task_metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                                    (f'{path.name}.created_by.task',)))

        self.metadata = {}
        self.new_metadata = {'task_control_name': task_control_name}
        self.task_metadata = {}

        self.changes = []
        self.content_has_changed = False
        self.need_write = False
        self._already_compared = False

    def load_metadata(self):
        if self.metadata_path.exists():
            self.metadata = try_json_read(self.metadata_path)
        else:
            raise NoMetadata(f'No metadata for {self.path}')

    def compare_path_with_previous(self):
        # I put the commented out lines to try to be smarter about when to check for changes.
        # Unfortunately they break the following:
        # t1 < in1 > out1
        # t2 < out1 > out2
        # modify in1 -> only t1 runs on remake (should be t1 and t2 if out1 gets modified)
        # if self._already_compared:
        #     return self.content_has_changed, self.need_write
        path = self.path
        logger.debug(f'comparing path with previous: {path}')
        self.content_has_changed = False
        self.need_write = False

        if self.metadata_path.exists():
            self.load_metadata()

        # N.B. lstat dereferences symlinks.
        stat = path.lstat()
        self.new_metadata.update({'st_size': stat.st_size, 'st_mtime': stat.st_mtime})
        stat_has_changed = False

        if self.metadata:
            if self.new_metadata['st_size'] != self.metadata['st_size']:
                stat_has_changed = True
                self.changes.append('st_size_changed')
            if self.new_metadata['st_mtime'] != self.metadata['st_mtime']:
                stat_has_changed = True
                self.changes.append('st_mtime_changed')

            if stat_has_changed:
                self.need_write = True
                # Only recalc sha1hex if size or last modified time have changed.
                sha1hex = sha1sum(path)
                self.new_metadata['sha1hex'] = sha1hex
                if sha1hex != self.metadata['sha1hex']:
                    logger.debug(f'{path} content has changed')
                    self.changes.append('sha1hex_changed')
                    self.content_has_changed = True
                else:
                    logger.debug(f'{path} properties have changed but contents the same')
            else:
                self.new_metadata['sha1hex'] = self.metadata['sha1hex']
        else:
            self.need_write = True

        self._already_compared = True
        return self.content_has_changed, self.need_write

    def compare_task_with_previous(self, task_path_hash_key):
        logger.debug(f'comparing task with previous: {self.path}')

        created_by_name_glob = f'{self.path.name}.created_by.*.task'
        created_by_name_paths = list(self.metadata_path.parent.glob(created_by_name_glob))

        if created_by_name_paths:
            assert len(created_by_name_paths), 'more than one created_by_task found.'
            created_by_name_path = created_by_name_paths[0]
            created_by_task_path_hash_key = created_by_name_path.name.split('.')[-2]
            if created_by_task_path_hash_key != task_path_hash_key:
                created_by_name_path.unlink()
                self.write_new_task_metadata(task_path_hash_key)
        else:
            self.write_new_task_metadata(task_path_hash_key)

    def write_new_metadata(self):
        if 'sha1hex' not in self.new_metadata:
            self.new_metadata['sha1hex'] = sha1sum(self.path)
        logger.debug(f'write new path metadata to {self.metadata_path}')
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write(self.new_metadata, self.metadata_path)
        self._already_compared = False

    def write_new_used_by_task_metadata(self, task_path_hash_key):
        used_by_name = f'{self.path.name}.used_by.{task_path_hash_key}.task'
        used_by_task_metadata_path = self.metadata_path.parent / used_by_name
        logger.debug(f'write input task metadata to {used_by_task_metadata_path}')
        used_by_task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        used_by_task_metadata_path.touch()

    def write_new_task_metadata(self, task_path_hash_key):
        logger.debug(f'write output task metadata to {self.task_metadata_path}')
        created_by_name = f'{self.path.name}.created_by.{task_path_hash_key}.task'
        created_by_task_metadata_path = self.metadata_path.parent / created_by_name
        created_by_task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        created_by_task_metadata_path.touch()
