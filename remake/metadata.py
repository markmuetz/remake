import json
from hashlib import sha1
from logging import getLogger

from remake.util import sha1sum


logger = getLogger(__name__)


class TaskMetadata:
    def __init__(self, dotremake_dir, task):
        self.dotremake_dir = dotremake_dir
        self.task = task
        self.inputs_metadata_map = {}
        self.outputs_metadata_map = {}
        self.metadata = {}
        self.requires_rerun = True
        self.task_metadata_dir = self.dotremake_dir / 'task_metadata'
        self.content_metadata_dir = self.dotremake_dir / 'content_metadata'
        self.rerun_reasons = []

    def generate_metadata(self):
        self.rerun_reasons = []
        for path in self.task.inputs:
            if not path.exists():
                self.rerun_reasons.append(('input_path_does_not_exist', path))
                self.requires_rerun = True
                return True
        for path in self.task.outputs:
            if not path.exists():
                self.rerun_reasons.append(('output_path_does_not_exist', path))
                self.requires_rerun = True
                return True

        task_sha1hex, content_sha1hex = self._task_content_sha1hex()

        self.metadata['task_sha1hex'] = task_sha1hex
        self.metadata['content_sha1hex'] = content_sha1hex

        self.requires_rerun = self.task_requires_rerun_based_on_content()
        return self.requires_rerun

    def _task_content_sha1hex(self):
        task_hash_data = []
        task_hash_data.append(self.task.func_source)
        if self.task.func_args:
            task_hash_data.append(str(self.task.func_args))
        if self.task.func_kwargs:
            task_hash_data.append(str(self.task.func_kwargs))

        content_hash_data = []
        for path in self.task.inputs:
            assert path.is_absolute()
            # Have already checked that path exists.
            input_path_md = InputPathMetadata(self.dotremake_dir, path)
            self.inputs_metadata_map[path] = input_path_md
            created, content_has_changed, needs_write = input_path_md.compare_input_with_previous()
            if content_has_changed:
                self.rerun_reasons.append(('content_has_changed', path))
            if needs_write:
                input_path_md.write_input()
            sha1hex = input_path_md.metadata['sha1hex']
            content_hash_data.append(sha1hex)

        task_sha1hex = sha1(''.join(task_hash_data).encode()).hexdigest()
        content_sha1hex = sha1(''.join(content_hash_data).encode()).hexdigest()
        return task_sha1hex, content_sha1hex

    def task_requires_rerun_based_on_content(self):
        task_sha1hex = self.metadata['task_sha1hex']
        content_sha1hex = self.metadata['content_sha1hex']
        requires_rerun = False
        for path in self.task.outputs:
            assert path.is_absolute()
            # Have already checked that path exists.
            output_path_md = OutputPathMetadata(self.dotremake_dir, path)
            self.outputs_metadata_map[path] = output_path_md
            requires_rerun = output_path_md.compare_output_with_previous(task_sha1hex, content_sha1hex)
            if requires_rerun:
                for reason in output_path_md.rerun_reasons:
                    self.rerun_reasons.append((reason, path))
                break

        return requires_rerun

    def write_output_metadata(self):
        self.task_metadata_dir.mkdir(parents=True, exist_ok=True)
        task_metadata_path = self.task_metadata_dir / self.metadata['task_sha1hex']
        if not task_metadata_path.exists():
            task_metadata_path.write_text(self.task.func_source)

        self.content_metadata_dir.mkdir(parents=True, exist_ok=True)
        content_metadata_path = self.content_metadata_dir / self.metadata['content_sha1hex']
        if not content_metadata_path.exists():
            content_data = [[str(p) for p in self.task.inputs]]
            content_metadata_path.write_text(json.dumps(content_data, indent=2) + '\n')
        else:
            # It is possible for the same content data to be used by 2 tasks.
            # Load the data, test whether or not the new content data is in there already, they write it back.
            content_data = json.loads(content_metadata_path.read_text())
            new_content_data = [str(p) for p in self.task.inputs]
            if new_content_data not in content_data:
                content_data.append(new_content_data)
            content_metadata_path.write_text(json.dumps(content_data, indent=2) + '\n')

        for output_path_md in self.outputs_metadata_map.values():
            output_path_md.write_output()


class InputPathMetadata:
    def __init__(self, dotremake_dir, path):
        self.dotremake_dir = dotremake_dir
        self.path = path
        self.metadata = {}
        self.prev_metadata = {}

        self.file_metadata_dir = self.dotremake_dir / 'file_metadata'
        self.metadata_path = self.file_metadata_dir.joinpath(*self.path.parts[1:])
        self.changes = []

        self.created = False
        self.content_has_changed = False
        self.need_write = False

    def compare_input_with_previous(self):
        path = self.path

        self.prev_metadata = None
        if self.metadata_path.exists():
            self.prev_metadata = json.loads(self.metadata_path.read_text())
        stat = path.stat()
        self.metadata = {'st_size': stat.st_size, 'st_mtime': stat.st_mtime}

        if self.prev_metadata:
            if self.metadata['st_size'] != self.prev_metadata['st_size']:
                self.content_has_changed = True
                self.changes.append('st_size_changed')
            if self.metadata['st_mtime'] != self.prev_metadata['st_mtime']:
                self.content_has_changed = True
                self.changes.append('st_mtime_changed')

            if self.content_has_changed:
                self.need_write = True
                # Only recalc sha1hex if size or last modified time have changed.
                sha1hex = sha1sum(path)
                self.metadata['sha1hex'] = sha1hex
                if sha1hex != self.prev_metadata['sha1hex']:
                    logger.debug(f'{path} has changed!')
                    self.changes.append('sha1hex_changed')
                    self.content_has_changed = True
                else:
                    logger.debug(f'{path} properties has changed but contents the same')
            else:
                self.metadata['sha1hex'] = self.prev_metadata['sha1hex']
        else:
            self.created = True
            self.metadata['sha1hex'] = sha1sum(path)
            self.need_write = True

        return self.created, self.content_has_changed, self.need_write

    def write_input(self):
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.write_text(json.dumps(self.metadata, indent=2) + '\n')


class OutputPathMetadata:
    def __init__(self, dotremake_dir, path):
        self.dotremake_dir = dotremake_dir
        self.path = path
        self.output_task_metadata = None
        self.prev_output_task_metadata = None

        self.file_metadata_dir = self.dotremake_dir / 'file_metadata'

        self.output_task_metadata_path = self.file_metadata_dir.joinpath(*(path.parent.parts[1:] +
                                                                           (f'{path.name}.task',)))
        self.rerun_reasons = []

    def compare_output_with_previous(self, task_sha1hex, content_sha1hex):
        self.output_task_metadata = {'task_sha1hex': task_sha1hex, 'content_sha1hex': content_sha1hex}
        if not self.path.exists():
            self.rerun_reasons.append('path_does_not_exist')
            return True

        requires_rerun = False
        if self.output_task_metadata_path.exists():
            self.prev_output_task_metadata = json.loads(self.output_task_metadata_path.read_text())
            if self.output_task_metadata['task_sha1hex'] != self.prev_output_task_metadata['task_sha1hex']:
                requires_rerun = True
                self.rerun_reasons.append('task_sha1hex_different')
            if self.output_task_metadata['content_sha1hex'] != self.prev_output_task_metadata['content_sha1hex']:
                requires_rerun = True
                self.rerun_reasons.append('content_sha1hex_different')
        return requires_rerun

    def write_output(self):
        self.output_task_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_task_metadata_path.write_text(json.dumps(self.output_task_metadata, indent=2) + '\n')


