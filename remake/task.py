from collections import Mapping
from hashlib import sha1
from pathlib import Path


class Task:
    def __init__(self, func, inputs, outputs,
                 func_args=[], func_kwargs={}):
        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs
        if not outputs:
            raise Exception('outputs must be set')

        if isinstance(inputs, Mapping):
            self.inputs_dict = {k: Path(v).absolute() for k, v in inputs.items()}
            self.inputs = [Path(i).absolute() for i in inputs.values()]
        else:
            self.inputs_dict = None
            self.inputs = [Path(i).absolute() for i in inputs]
        if isinstance(outputs, Mapping):
            self.outputs_dict = {k: Path(v).absolute() for k, v in outputs.items()}
            self.outputs = [Path(o).absolute() for o in outputs.values()]
        else:
            self.outputs_dict = None
            self.outputs = [Path(o).absolute() for o in outputs]
        self.result = None
        self.rerun_on_mtime = True

    def __repr__(self):
        return f'Task({self.func.__code__.co_name}, {[f.name for f in self.inputs]}, {[f.name for f in self.outputs]})'

    def can_run(self):
        can_run = True
        for input_path in self.inputs:
            if not input_path.exists():
                can_run = False
                break
        return can_run

    def requires_rerun(self):
        rerun = False
        earliest_output_path_mtime = float('inf')
        for output in self.outputs:
            if not Path(output).exists():
                rerun = True
                break
            earliest_output_path_mtime = min(earliest_output_path_mtime,
                                             output.stat().st_mtime)
        if self.rerun_on_mtime and not rerun:
            latest_input_path_mtime = 0
            for input_path in self.inputs:
                latest_input_path_mtime = max(latest_input_path_mtime,
                                              input_path.stat().st_mtime)
            if latest_input_path_mtime > earliest_output_path_mtime:
                rerun = True

        return rerun

    def complete(self):
        for output in self.outputs:
            if not output.exists():
                return False
        return True

    def hexdigest(self):
        h = sha1(self.func.__code__.co_name.encode())
        for input_path in self.inputs:
            h.update(str(input_path).encode())
        for output_path in self.outputs:
            h.update(str(output_path).encode())
        return h.hexdigest()

    def run(self, force=False):
        if not self.can_run():
            raise Exception('Not all files required for task exist')

        if self.requires_rerun() or force:
            for output_dir in set([o.parent for o in self.outputs]):
                output_dir.mkdir(parents=True, exist_ok=True)
            inputs = self.inputs_dict if self.inputs_dict else self.inputs
            outputs = self.outputs_dict if self.outputs_dict else self.outputs
            self.result = self.func(inputs, outputs, *self.func_args, **self.func_kwargs)
            for output in self.outputs:
                if not output.exists():
                    raise Exception(f'func {output} not created')
        else:
            print(f'  Already exist: {self.outputs}')

        return self

