import difflib
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
    last_run_status: int = 0
    last_run_timestamp: int = 0  # TODO: should be timestamp.
    last_run_code: str = ''
    inputs_missing: bool = False

    def __init__(self, rule, inputs, outputs, kwargs):
        self.rule = rule
        self.inputs = inputs
        if not outputs:
            raise ValueError('Task.outputs must be set')
        self.outputs = outputs

        # Makes task queryable using QueryList
        for k, v in kwargs.items():
            if not isinstance(k, str):
                raise ValueError('Task.kwargs keys must be strings')
            setattr(self, k, v)
        self.kwargs = kwargs

        self.prev_tasks = []
        self.next_tasks = []

    def __post_init__(self):
        if not self.outputs:
            raise ValueError('Task.outputs must be set')

    def run(self):
        self.rule.run_task(self)

    def rule_name(self):
        return self.rule.__name__

    def __hash__(self):
        if not hasattr(self, '_hash'):
            self._hash = hash(
                ','.join(str(v) for v in self.inputs.values())
                + ','.join(str(v) for v in self.outputs.values())
            )
        return self._hash

    def key(self):
        if not hasattr(self, '_key'):
            self._key = sha1(
                (
                    ','.join(str(v) for v in self.inputs.values())
                    + ','.join(str(v) for v in self.outputs.values())
                ).encode()
            ).hexdigest()
        return self._key

    def diff(self):
        return list(
            difflib.ndiff(self.last_run_code.split('\n'), self.rule.source['rule_run'].split('\n'))
        )

    def __repr__(self):
        # return (f'{self.key()[:8]} Task(rule={self.rule.__name__}, inputs={len(self.inputs)}, outputs={len(self.outputs)}, '
        #         f'kwargs={self.kwargs}, prev_tasks={len(self.prev_tasks)}, next_tasks={len(self.next_tasks)})')
        # return (f'{self.key()[:8]} Task({self.rule.__name__}, {self.kwargs})')
        kstr = ', '.join(f'{k}={v}' for k, v in self.kwargs.items())
        return f'{self.key()[:8]} {self.rule.__name__}[{kstr}]'
