import os
import datetime as dt
import json
from hashlib import sha1
from logging import getLogger
from time import sleep

from remake.util import sha1sum


logger = getLogger(__name__)

METADATA_VERSION = 'metadata_v2'
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


class TaskMetadata:
    def __init__(self, dotremake_dir, task):
        self.dotremake_dir = dotremake_dir
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.task = task
        self.task_path_hash_key = self.task.path_hash_key()

        self.inputs_metadata_map = {}
        self.outputs_metadata_map = {}
        self.metadata = {}
        self.new_metadata = {}
        self.requires_rerun = True
        self.rerun_reasons = []
        self.task_metadata_dir_path = None
        self.log_path = None

        self.task_metadata_dir = self.metadata_dir / 'task_metadata'
        self.task_metadata_dir_path = self.task_metadata_dir / self.task_path_hash_key

        self.task_metadata_path = self.task_metadata_dir_path / 'task.metadata'
        self.log_path = self.task_metadata_dir_path / 'task.log'

        self.task_status_path = self.metadata_dir / 'task_status' / (self.task_path_hash_key + '.status')

        for input_path in self.task.inputs:
            self.inputs_metadata_map[input_path] = PathMetadata(self.dotremake_dir, input_path)

        for output_path in self.task.outputs:
            self.outputs_metadata_map[output_path] = PathMetadata(self.dotremake_dir, output_path)

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

        task_sha1hex = self._task_sha1hex()
        content_sha1hex = self._content_sha1hex()
        if content_sha1hex is None:
            logger.debug(f'no existing content')
            self.rerun_reasons.append(('no_existing_input_paths', None))
            return False

        self.new_metadata['task_sha1hex'] = task_sha1hex
        self.new_metadata['content_sha1hex'] = content_sha1hex
        return True

    def _task_sha1hex(self):
        task_hash_data = [self.task.func_source]
        if self.task.func_args:
            task_hash_data.append(str(self.task.func_args))
        if self.task.func_kwargs:
            task_hash_data.append(str(self.task.func_kwargs))
        task_sha1hex = sha1(''.join(task_hash_data).encode()).hexdigest()
        return task_sha1hex

    def _content_sha1hex(self):
        content_hash_data = []
        for path in self.task.inputs:
            assert path.is_absolute()
            if not path.exists():
                logger.debug(f'no path exists: {path}')
                return None
            input_path_md = self.inputs_metadata_map[path]
            content_has_changed, _ = input_path_md.compare_path_with_previous()
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

        self.requires_rerun = False
        self.rerun_reasons = []
        try:
            self.load_metadata()
        except NoMetadata:
            self.rerun_reasons.append(('task_has_not_been_run', None))
            self.requires_rerun = True

        if not self.requires_rerun:
            for path in self.task.inputs:
                if not path.exists():
                    self.rerun_reasons.append(('input_path_does_not_exist', path))
                    self.requires_rerun = True
                    break

        if not self.requires_rerun:
            for path in self.task.outputs:
                if not path.exists():
                    self.rerun_reasons.append(('output_path_does_not_exist', path))
                    self.requires_rerun = True
                    break

        if not self.requires_rerun:
            if self.new_metadata['task_sha1hex'] != self.metadata['task_sha1hex']:
                self.requires_rerun = True
                self.rerun_reasons.append('task_sha1hex_different')
            if self.new_metadata['content_sha1hex'] != self.metadata['content_sha1hex']:
                self.requires_rerun = True
                self.rerun_reasons.append('content_sha1hex_different')

        logger.debug(f'task requires rerun {self.requires_rerun}: {self.task_path_hash_key}')
        return self.requires_rerun

    def write_output_metadata(self):
        logger.debug(f'write output metadata {self.task_path_hash_key}')
        self.task_metadata_dir_path.mkdir(parents=True, exist_ok=True)
        task_func_path = self.task_metadata_dir_path / 'func_source.py'

        logger.debug(f'write task source to {task_func_path}')
        task_func_path.write_text(self.task.func_source)

        inputs_outputs_path = self.task_metadata_dir_path / 'inputs_outputs'
        task_inputs = [str(p) for p in self.task.inputs]
        task_outputs = [str(p) for p in self.task.outputs]
        logger.debug(f'write inputs/outputs to {inputs_outputs_path}')
        flush_json_write({'inputs': task_inputs, 'outputs': task_outputs}, inputs_outputs_path)

        logger.debug(f'write task metadata to {self.task_metadata_path}')
        flush_json_write(self.new_metadata, self.task_metadata_path)

        for input_path_md in self.inputs_metadata_map.values():
            input_path_md.write_new_used_by_task_metadata(self.task_path_hash_key)

        for output_path_md in self.outputs_metadata_map.values():
            _, needs_write = output_path_md.compare_path_with_previous()
            if needs_write:
                output_path_md.write_new_metadata()
            _, needs_write = output_path_md.compare_task_with_previous(self.task_path_hash_key)
            if needs_write:
                output_path_md.write_new_task_metadata()


class PathMetadata:
    def __init__(self, dotremake_dir, path):
        self.dotremake_dir = dotremake_dir
        self.path = path
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.file_metadata_dir = self.metadata_dir / 'file_metadata'

        self.metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                               (f'{path.name}.metadata',)))
        self.task_metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                                    (f'{path.name}.created_by_task',)))

        self.metadata = {}
        self.new_metadata = {}
        self.task_metadata = {}
        self.new_task_metadata = {}

        self.changes = []

        self.content_has_changed = False
        self.task_has_changed = False
        self.need_write = False

    def load_metadata(self):
        if self.metadata_path.exists():
            self.metadata = try_json_read(self.metadata_path)
        else:
            raise NoMetadata(f'No metadata for {self.path}')

    def load_task_metadata(self):
        if self.task_metadata_path.exists():
            self.task_metadata = try_json_read(self.task_metadata_path)
        else:
            raise NoMetadata(f'No task metadata for: {self.path}')

    def compare_path_with_previous(self):
        path = self.path
        logger.debug(f'comparing path with previous: {path}')

        if self.metadata_path.exists():
            self.load_metadata()

        # N.B. lstat dereferences symlinks.
        stat = path.lstat()
        self.new_metadata = {'st_size': stat.st_size, 'st_mtime': stat.st_mtime}
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

        return self.content_has_changed, self.need_write

    def compare_task_with_previous(self, task_path_hash_key):
        logger.debug(f'comparing task with previous: {self.path}')

        if self.task_metadata_path.exists():
            self.load_task_metadata()
            if self.task_metadata['task_path_hash_key'] != task_path_hash_key:
                self.changes.append('task_path_hash_key_changed')
                self.task_has_changed = True
                self.need_write = True
        else:
            self.need_write = True
        self.new_task_metadata['task_path_hash_key'] = task_path_hash_key

        return self.task_has_changed, self.need_write

    def write_new_metadata(self):
        if 'sha1hex' not in self.new_metadata:
            self.new_metadata['sha1hex'] = sha1sum(self.path)
        logger.debug(f'write input metadata to {self.metadata_path}')
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write(self.new_metadata, self.metadata_path)

    def write_new_used_by_task_metadata(self, task_path_hash_key):
        used_by_name = f'{self.path.name}.used_by.{task_path_hash_key}.task'
        used_by_task_metadata_path = self.metadata_path.parent / used_by_name
        logger.debug(f'write input task metadata to {used_by_task_metadata_path}')
        used_by_task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write({'task': task_path_hash_key}, used_by_task_metadata_path)

    def write_new_task_metadata(self):
        assert 'task_path_hash_key' in self.new_task_metadata
        logger.debug(f'write output task metadata to {self.task_metadata_path}')
        self.task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write(self.new_task_metadata, self.task_metadata_path)
