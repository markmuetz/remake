from dataclasses import dataclass
from hashlib import sha1
import pathlib

from .rule import Rule

@dataclass
class Task:
    rule: Rule
    inputs: dict
    outputs: dict
    kwargs: dict
    prev_tasks: list
    next_tasks: list
    is_run: bool = False

    def run(self):
        # for task in self.prev_tasks:
        #     assert task.is_run == True
        for input_ in self.inputs.values():
            # assert output in VirtFS
            assert pathlib.Path(input_).exists(), f'{input_} does not exist'
        ret = self.rule.run_task(self)
        for output in self.outputs.values():
            # assert output in VirtFS
            assert pathlib.Path(output).exists(), f'{output} not created'
        self.is_run = True
        self.requires_rerun = False
        self.rule.remake.update_task(self)
        return ret

    def __hash__(self):
        if not hasattr(self, '_hash'):
            self._hash = hash(
                ','.join(self.inputs.values()) +
                ','.join(self.outputs.values())
            )
        return self._hash

    def key(self):
        if not hasattr(self, '_key'):
            self._key = sha1(
                (','.join(self.inputs.values()) +
                 ','.join(self.outputs.values())).encode()
            ).hexdigest()
        return self._key

    def __repr__(self):
        return (f'{self.key()[:8]} Task(rule={self.rule.__name__}, inputs={len(self.inputs)}, outputs={len(self.outputs)}, '
                f'kwargs={self.kwargs}, prev_tasks={len(self.prev_tasks)}, next_tasks={len(self.next_tasks)})')


