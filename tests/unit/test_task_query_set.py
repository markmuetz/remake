import unittest
from unittest import mock
from itertools import product

from remake.task_query_set import TaskQuerySet


class TestTaskQuerySet(unittest.TestCase):
    def setUp(self) -> None:
        tasks = []
        statuses = {'a': 'completed', 'b': 'pending', 'c': 'remaining'}
        for val1, val2 in product(['a', 'b', 'c'], [1, 2, 3]):
            task = mock.MagicMock()
            task.val1 = val1
            task.val2 = val2
            del task.val3
            task.path_hash_key.return_value = ''.join([val1, str(val2)] * 20)
            task.__str__.return_value = f'Task(val1={val1}, val2={val2})'
            task.__class__.__name__ = 'Rule' + val1.upper()
            task.status = statuses[val1]
            task.task_md.rerun_reasons = [('func_changed', None),
                                          ('path_doesnt_exist', 'not/there.txt')]
            task.diff.return_value = ['def fn(self)', '+    print("hi")', '    pass']
            tasks.append(task)

        task_ctrl = mock.MagicMock()
        self.tasks = TaskQuerySet(tasks, task_ctrl)
        assert len(self.tasks) == 9

    def test_index_slice(self):
        self.tasks[0]
        self.tasks[:3]
        self.tasks[:-1]

    def test_filter(self):
        self.assertEqual(len(self.tasks.filter(val1='a')), 3)
        self.assertEqual(len(self.tasks.filter(val1='a', val2=3)), 1)
        self.assertEqual(len(self.tasks.filter(val1='a', val2='3', cast_to_str=True)), 1)
        self.assertEqual(len(self.tasks.exclude(val1='b')), 6)

    def test_get(self):
        task = self.tasks.get(val1='a', val2=2)
        self.assertEqual(task.val1, 'a')
        self.assertEqual(task.val2, 2)
        self.assertRaises(Exception, self.tasks.get, val1='a')
        self.assertRaises(Exception, self.tasks.get, val1='d')

    def test_first_last(self):
        task = self.tasks.first()
        self.assertEqual(task.val1, 'a')
        self.assertEqual(task.val2, 1)
        task = self.tasks.last()
        self.assertEqual(task.val1, 'c')
        self.assertEqual(task.val2, 3)
        no_tasks = self.tasks[:0]
        self.assertRaises(Exception, no_tasks.first)
        self.assertRaises(Exception, no_tasks.last)

    def test_in_rule(self):
        ruleAtasks = self.tasks.in_rule('RuleA')
        self.assertEqual(len(ruleAtasks), 3)
        rule = mock.MagicMock()

        for task in ruleAtasks:
            task.__class__ = rule

        ruleAtasks2 = self.tasks.in_rule(rule)
        self.assertEqual(len(ruleAtasks2), 3)

    def test_run(self):
        self.tasks.run()

    def test_status(self):
        self.tasks.status()
        self.tasks.status(True)
        self.tasks.status(True, True)
