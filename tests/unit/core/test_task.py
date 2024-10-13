from itertools import product
import unittest
from unittest import mock

from remake import Task

class Rule:
    @staticmethod
    def run_task(task):
        pass

    source = {'rule_run': 'def rule_run(inputs, outputs):\n    print(inputs)'}


class TestTask(unittest.TestCase):
    def test_task_fns(self):
        task = Task(Rule, inputs={}, outputs={'out': 'out1'}, kwargs={})
        str(task)
        task.key()
        task.run()
        task.rule_name()

    def test_task_output_not_set(self):
        with self.assertRaises(ValueError):
            task = Task(Rule, inputs={}, outputs={}, kwargs={})

    def test_task_diff(self):
        task = Task(Rule, inputs={}, outputs={'out': 'out1'}, kwargs={})
        task.last_run_code = 'def rule_run(inputs, outputs):\n    print(outputs)'
        diff = task.diff()
        assert '-     print(outputs)' in diff
        assert '+     print(inputs)' in diff

    def test_task_keys_different(self):
        tasks = []
        for inputs, outputs in product(
                [{}, {'in': 'in1'}, {'in': 'in2'}, {'in': 'in2', 'other_in': 'in3'}],
                [{'out': 'out1'}, {'out': 'out2'}, {'out': 'out2', 'other_out': 'out3'}]
        ):
            tasks.append(Task(Rule, inputs=inputs, outputs=outputs, kwargs={}))

        task_keys = [t.key() for t in tasks]
        assert len(task_keys) == len(set(task_keys))

    def test_task_hash(self):
        task_dict = {}
        for inputs, outputs in product(
                [{}, {'in': 'in1'}, {'in': 'in2'}, {'in': 'in2', 'other_in': 'in3'}],
                [{'out': 'out1'}, {'out': 'out2'}, {'out': 'out2', 'other_out': 'out3'}]
        ):
            task = Task(Rule, inputs=inputs, outputs=outputs, kwargs={})
            hash(task)
            task_dict[task] = 1

    def test_task_kwargs(self):
        task = Task(Rule, inputs={}, outputs={'out': 'out1'}, kwargs={'a': 1, 'b': 'two'})
        assert task.a == 1
        assert task.b == 'two'

    def test_task_kwargs_key_not_string(self):
        with self.assertRaises(ValueError):
            task = Task(Rule, inputs={}, outputs={'out': 'out1'}, kwargs={2: 1, 'b': 'two'})


