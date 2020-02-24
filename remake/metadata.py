import os
import json
from hashlib import sha1
from logging import getLogger
from time import sleep

from remake.util import sha1sum


logger = getLogger(__name__)

METADATA_VERSION = 'metadata_v1'
JSON_READ_RETRIES = 3


def flush_json_write(obj, path):
    with path.open('w') as fp:
        json.dump(obj, fp, indent=2)
        fp.write('\n')
        fp.flush()
        os.fsync(fp)


def try_json_read(path):
    retries = JSON_READ_RETRIES

    while retries:
        retries -= 1
        try:
            return json.loads(path.read_text())
        except Exception as e:
            logger.error(e)
            logger.debug(path)
            logger.debug(f'retries: {retries}')
            if not retries:
                raise
        sleep(5)


class TaskMetadata:
    def __init__(self, dotremake_dir, task):
        self.dotremake_dir = dotremake_dir
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.task = task
        self.inputs_metadata_map = {}
        self.outputs_metadata_map = {}
        self.metadata = {}
        self.requires_rerun = True
        self.task_metadata_dir = self.metadata_dir / 'task_metadata'
        self.content_metadata_dir = self.metadata_dir / 'content_metadata'
        self.rerun_reasons = []
        self.task_metadata_dir_path = None
        self.log_path = None

    def generate_metadata(self):
        logger.debug(f'generate metadata for {self.task.path_hash_key()}')
        self.rerun_reasons = []
        for path in self.task.inputs:
            if not path.exists():
                self.rerun_reasons.append(('input_path_does_not_exist', path))
                self.requires_rerun = True
                return True

        task_sha1hex = self._task_sha1hex()
        content_sha1hex = self._content_sha1hex()

        self.metadata['task_sha1hex'] = task_sha1hex
        self.metadata['content_sha1hex'] = content_sha1hex
        self.task_metadata_dir_path = self.task_metadata_dir / task_sha1hex
        # TODO: 2 tasks can have identical function and identical content, but still be different and require
        # TODO: different log dirs. How to handle??
        # Original:
        # self.log_path = self.task_metadata_dir_path / f'{content_sha1hex}_task.log'
        # What about just making the log_path:
        self.metadata['task_path_hash_key'] = self.task.path_hash_key()
        self.log_path = self.task_metadata_dir_path / f'{self.metadata["task_path_hash_key"]}_task.log'

        for path in self.task.outputs:
            if not path.exists():
                self.rerun_reasons.append(('output_path_does_not_exist', path))
                self.requires_rerun = True
                return True

        self.requires_rerun = self.task_requires_rerun_based_on_content()
        return self.requires_rerun

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
            # Have already checked that path exists.
            input_path_md = PathMetadata(self.dotremake_dir, path)
            self.inputs_metadata_map[path] = input_path_md
            created, content_has_changed, needs_write = input_path_md.compare_path_with_previous()
            if content_has_changed:
                self.rerun_reasons.append(('content_has_changed', path))
            sha1hex = input_path_md.input_metadata['sha1hex']
            content_hash_data.append(sha1hex)

        content_sha1hex = sha1(''.join(content_hash_data).encode()).hexdigest()
        return content_sha1hex

    def task_requires_rerun_based_on_content(self):
        task_sha1hex = self.metadata['task_sha1hex']
        content_sha1hex = self.metadata['content_sha1hex']
        task_path_hash_key = self.metadata['task_path_hash_key']
        requires_rerun = False
        for path in self.task.outputs:
            assert path.is_absolute()
            # Have already checked that path exists.
            output_path_md = PathMetadata(self.dotremake_dir, path)
            self.outputs_metadata_map[path] = output_path_md
            requires_rerun = output_path_md.compare_output_with_previous(task_sha1hex,
                                                                         content_sha1hex,
                                                                         task_path_hash_key)
            if requires_rerun:
                for reason in output_path_md.rerun_reasons:
                    self.rerun_reasons.append((reason, path))
                break

        logger.debug(f'tasks requires rerun {requires_rerun}: {self.task.path_hash_key()}')
        return requires_rerun

    def write_output_metadata(self):
        logger.debug(f'write output metadata {self.task.path_hash_key()}')
        self.task_metadata_dir_path.mkdir(parents=True, exist_ok=True)
        task_func_path = self.task_metadata_dir_path / 'func_source.py'
        logger.debug(f'write task metadata to {task_func_path}')
        if not task_func_path.exists():
            task_func_path.write_text(self.task.func_source)

        self.content_metadata_dir.mkdir(parents=True, exist_ok=True)
        content_metadata_path = self.content_metadata_dir / self.metadata['content_sha1hex']
        logger.debug(f'write content metadata to {content_metadata_path}')
        if not content_metadata_path.exists():
            content_data = [[str(p) for p in self.task.inputs]]
            flush_json_write(content_data, content_metadata_path)
        else:
            # It is possible for the same content data to be used by 2 tasks.
            # Load the data, test whether or not the new content data is in there already, they write it back.
            # TODO: This introduces a problem.
            # TODO: Every output file has a link back to the content that created it through its content_sha1hex.
            # TODO: But in this case it is non-unique -- instead of finding a path it finds a list, any of which could
            # TODO: be the original content. Granted, they are identical, but still not ideal.
            content_data = try_json_read(content_metadata_path)
            new_content_data = [str(p) for p in self.task.inputs]
            if new_content_data not in content_data:
                content_data.append(new_content_data)
            flush_json_write(content_data, content_metadata_path)

        for output_path_md in self.outputs_metadata_map.values():
            _, _, needs_write = output_path_md.compare_path_with_previous()
            if needs_write:
                output_path_md.write_path_metadata()
            output_path_md.write_task_metadata()


class PathMetadata:
    def __init__(self, dotremake_dir, path):
        self.dotremake_dir = dotremake_dir
        self.metadata_dir = dotremake_dir / METADATA_VERSION
        self.path = path
        self.input_metadata = {}
        self.prev_input_metadata = {}

        self.output_task_metadata = {}
        self.prev_output_task_metadata = {}

        self.file_metadata_dir = self.metadata_dir / 'file_metadata'

        self.metadata_path = self.file_metadata_dir.joinpath(*self.path.parts[1:])
        self.output_task_metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                                           (f'{path.name}.task',)))
        self.changes = []
        self.rerun_reasons = []

        self.created = False
        self.content_has_changed = False
        self.need_write = False

    def compare_path_with_previous(self):
        path = self.path
        logger.debug(f'comparing path with previous: {path}')

        self.prev_input_metadata = None
        if self.metadata_path.exists():
            self.prev_input_metadata = try_json_read(self.metadata_path)
        # N.B. lstat dereferences symlinks.
        # Think using path.stat() was causing JSONreads bug.
        stat = path.lstat()
        self.input_metadata = {'st_size': stat.st_size, 'st_mtime': stat.st_mtime}
        stat_has_changed = False

        if self.prev_input_metadata:
            if self.input_metadata['st_size'] != self.prev_input_metadata['st_size']:
                stat_has_changed = True
                self.changes.append('st_size_changed')
            if self.input_metadata['st_mtime'] != self.prev_input_metadata['st_mtime']:
                stat_has_changed = True
                self.changes.append('st_mtime_changed')

            if stat_has_changed:
                self.need_write = True
                # Only recalc sha1hex if size or last modified time have changed.
                sha1hex = sha1sum(path)
                self.input_metadata['sha1hex'] = sha1hex
                if sha1hex != self.prev_input_metadata['sha1hex']:
                    logger.debug(f'{path} content has changed')
                    self.changes.append('sha1hex_changed')
                    self.content_has_changed = True
                else:
                    logger.debug(f'{path} properties have changed but contents the same')
            else:
                self.input_metadata['sha1hex'] = self.prev_input_metadata['sha1hex']
        else:
            self.created = True
            self.input_metadata['sha1hex'] = sha1sum(path)
            self.need_write = True

        return self.created, self.content_has_changed, self.need_write

    def write_path_metadata(self):
        logger.debug(f'write input metadata to {self.metadata_path}')
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        flush_json_write(self.input_metadata, self.metadata_path)

    def compare_output_with_previous(self, task_sha1hex, content_sha1hex, task_path_hash_key):
        logger.debug(f'comparing output with previous: {self.path}')
        self.output_task_metadata = {
            'task_sha1hex': task_sha1hex,
            'content_sha1hex': content_sha1hex,
            'task_path_hash_key': task_path_hash_key,
        }
        if not self.path.exists():
            self.rerun_reasons.append('path_does_not_exist')
            return True

        requires_rerun = False
        if self.output_task_metadata_path.exists():
            # bug: JSONreads
            self.prev_output_task_metadata = try_json_read(self.output_task_metadata_path)
            if self.output_task_metadata['task_sha1hex'] != self.prev_output_task_metadata['task_sha1hex']:
                requires_rerun = True
                self.rerun_reasons.append('task_sha1hex_different')
            if self.output_task_metadata['content_sha1hex'] != self.prev_output_task_metadata['content_sha1hex']:
                requires_rerun = True
                self.rerun_reasons.append('content_sha1hex_different')
        return requires_rerun

    def write_task_metadata(self):
        logger.debug(f'write output metadata to {self.output_task_metadata_path}')
        self.output_task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        # bug: JSONreads
        # Don't think flushing is the problem.
        # Or perhaps it is. Do flushed writes for all json.
        flush_json_write(self.output_task_metadata, self.output_task_metadata_path)
