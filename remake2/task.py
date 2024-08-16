from dataclasses import dataclass
from hashlib import sha1

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
        self.rule.run_task(self)

    def __hash__(self):
        if not hasattr(self, '_hash'):
            self._hash = hash(
                ','.join(str(v) for v in self.inputs.values()) +
                ','.join(str(v) for v in self.outputs.values())
            )
        return self._hash

    def key(self):
        if not hasattr(self, '_key'):
            self._key = sha1(
                (','.join(str(v) for v in self.inputs.values()) +
                 ','.join(str(v) for v in self.outputs.values())).encode()
            ).hexdigest()
        return self._key

    def __repr__(self):
        # return (f'{self.key()[:8]} Task(rule={self.rule.__name__}, inputs={len(self.inputs)}, outputs={len(self.outputs)}, '
        #         f'kwargs={self.kwargs}, prev_tasks={len(self.prev_tasks)}, next_tasks={len(self.next_tasks)})')
        return (f'{self.key()[:8]} Task({self.rule.__name__}, {self.kwargs})')


