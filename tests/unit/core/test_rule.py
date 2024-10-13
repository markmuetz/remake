import os
import pathlib
from itertools import product
import unittest
from unittest import mock

from remake import Task, Rule
from remake.core.rule import tmp_atomic_path
from remake.core.remake_exceptions import RemakeOutputNotCreated

class R1(Rule):
    remake = mock.Mock()
    remake.config = {'old_style_class': False}

    @staticmethod
    def rule_run(inputs, outputs):
        pass

class RE(Rule):
    remake = mock.Mock()
    remake.config = {'old_style_class': False}

    @staticmethod
    def rule_run(inputs, outputs):
        raise Exception('Oh dear!')

def exist_factory(paths):
    def my_exists(self):
        return str(self) in paths
    return my_exists


class TestRule(unittest.TestCase):
    def test_rule_tmp_atomic_path(self):
        outfn = 'out1'
        outtmpfn = str(tmp_atomic_path(outfn))
        assert outtmpfn == f'.remake.tmp.{outfn}'

    def test_rule_run_task(self):
        remake = mock.Mock()
        outfn = 'out1'
        task = Task(R1, inputs={}, outputs={'out': outfn}, kwargs={})

        outtmpfn = str(tmp_atomic_path(outfn))
        with (
            mock.patch.object(pathlib.Path, 'exists', exist_factory([outtmpfn])),
            mock.patch.object(os, 'rename', lambda x, y: None),
        ):
            task.run()
            R1.remake.update_task.assert_called_with(task)
            assert task.last_run_status == 1

            R1.run_task(task)
            R1.remake.update_task.assert_called_with(task)

            R1.remake.update_task.reset_mock()
            R1.run_task(task, save_status=False)
            R1.remake.update_task.assert_not_called()

    def test_rule_run_task_output_not_created(self):
        remake = mock.Mock()
        outfn = 'out1'
        task = Task(R1, inputs={}, outputs={'out': outfn}, kwargs={})

        with self.assertRaises(RemakeOutputNotCreated):
            task.run()

    def test_rule_run_task_exception(self):
        remake = mock.Mock()
        outfn = 'out1'
        task = Task(RE, inputs={}, outputs={'out': outfn}, kwargs={})
        with self.assertRaises(Exception) as cm:
            task.run()
        assert task.last_run_status == 2
        # Hard to figure out the exact arg for the exception.
        RE.remake.update_task.assert_called()

        RE.remake.update_task.reset_mock()
        with self.assertRaises(Exception):
            RE.run_task(task, save_status=False)
        RE.remake.update_task.assert_not_called()

