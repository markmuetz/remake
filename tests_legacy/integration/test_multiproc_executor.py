import os
import unittest
from pathlib import Path
from unittest import mock

from remake.util import sysrun
from remake.loader import load_remake
from remake.executor.multiproc_executor import worker


examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


class TestSlurmExecutorWorker(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')

    def tearDown(self) -> None:
        sysrun('make reset')
        os.chdir(self.orig_cwd)

    def test1_worker_run_no_jobs(self):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()

        task_queue = mock.MagicMock()
        task_queue.get.side_effect = [None]
        task_complete_queue = mock.MagicMock()

        worker(1, remakefile, task_queue, task_complete_queue)

    def test2_worker_run_rescan(self):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()
        rescan_task = remake.task_ctrl.rescan_tasks[0]

        task_queue = mock.MagicMock()
        task_queue.get.side_effect = [('rescan', rescan_task.path_hash_key(), False), None]
        task_complete_queue = mock.MagicMock()

        worker(1, remakefile, task_queue, task_complete_queue)

    def test3_worker_run_task(self):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()
        task = remake.tasks[0]

        task_queue = mock.MagicMock()
        task_queue.get.side_effect = [('task', task.path_hash_key(), False), None]
        task_complete_queue = mock.MagicMock()

        worker(1, remakefile, task_queue, task_complete_queue)

    @mock.patch('remake.task.Task.run')
    def test4_worker_exception(self, mock_run):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()
        task = remake.tasks[0]

        task_queue = mock.MagicMock()
        task_queue.get.side_effect = [('rescan', task.path_hash_key(), False), None]
        task_complete_queue = mock.MagicMock()
        mock_run.return_value = Exception('Boom!')

        self.assertRaises(Exception, worker(1, remakefile, task_queue, task_complete_queue))
