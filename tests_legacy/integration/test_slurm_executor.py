import os
import unittest
from hashlib import sha1
from pathlib import Path
from unittest import mock
import subprocess as sp

from remake.util import sysrun
from remake.loader import load_remake
from remake.executor.slurm_executor import run_job


examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


class TestSlurmExecutor(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')
        self.remake = load_remake('ex3.py')
        self.remake.finalize()

    def tearDown(self) -> None:
        sysrun('make reset')
        os.chdir(self.orig_cwd)

    @mock.patch('remake.executor.slurm_executor.sysrun')
    def test_slurm_executor(self, mock_sysrun):
        # Mock first call to sysrun -- squeue.
        mock_ret = mock.MagicMock()
        mock_ret.stdout = '\n'.join([
            '             JOBID            PARTITION       NAME ST',
            '            123456               queue1 abcdefghij  R'
        ])
        sysrun_ret = [mock_ret]
        # Mock first call to sysrun -- sbatch.
        for i in range(len(self.remake.task_ctrl.rescan_tasks),
                       len(self.remake.tasks)):
            mock_ret = mock.MagicMock()
            mock_ret.stdout = f'Submitted batch job {i + 100000}'
            sysrun_ret.append(mock_ret)
        mock_sysrun.side_effect = sysrun_ret
        self.remake.task_ctrl.set_executor('slurm')
        self.remake.run_all()

    @mock.patch('remake.executor.slurm_executor.sysrun')
    def test_slurm_executor_error(self, mock_sysrun):
        # Mock first call to sysrun -- squeue.
        mock_ret = mock.MagicMock()
        mock_ret.stdout = '\n'.join([
            '             JOBID            PARTITION       NAME ST',
            '            123456               queue1 abcdefghij PD'
        ])
        sysrun_ret = [mock_ret]
        # Mock first call to sysrun -- sbatch.
        for i in range(len(self.remake.task_ctrl.rescan_tasks),
                       len(self.remake.tasks)):
            mock_ret = mock.MagicMock()
            mock_ret.stdout = f'Submitted batch job {i + 100000}'
            sysrun_ret.append(mock_ret)
        sysrun_ret[len(sysrun_ret) // 2] = sp.CalledProcessError(100, 'cmd')
        mock_sysrun.side_effect = sysrun_ret

        self.remake.task_ctrl.set_executor('slurm')
        self.assertRaises(sp.CalledProcessError, self.remake.run_all)

    @mock.patch('remake.executor.slurm_executor.sysrun')
    def test_slurm_remake_run_requested(self, mock_sysrun):
        # Mock first call to sysrun -- squeue.
        mock_ret = mock.MagicMock()
        mock_ret.stdout = '\n'.join([
            '             JOBID            PARTITION       NAME ST',
            '            123456               queue1 abcdefghij  R'
        ])
        sysrun_ret = [mock_ret]
        # Mock first call to sysrun -- sbatch.
        for i in range(len(self.remake.task_ctrl.rescan_tasks),
                       len(self.remake.tasks)):
            mock_ret = mock.MagicMock()
            mock_ret.stdout = f'Submitted batch job {i + 100000}'
            sysrun_ret.append(mock_ret)
        sysrun_ret[len(sysrun_ret) // 2] = sp.CalledProcessError(100, 'cmd')
        mock_sysrun.side_effect = sysrun_ret

        self.remake.task_ctrl.set_executor('slurm')
        self.remake.run_requested(self.remake.tasks[:3], handle_dependencies=True)

    @mock.patch('remake.executor.slurm_executor.SlurmExecutor._task_already_queued_running')
    @mock.patch('remake.executor.slurm_executor.sysrun')
    def test_slurm_executor_already_running(self, mock_sysrun, mock_already):
        # First, pick a task to be already running.
        running_task = self.remake.tasks[len(self.remake.tasks) // 2]
        task_key = running_task.path_hash_key()[:10]
        # Mock first call to sysrun -- squeue. Give it the task_key of the running task.
        mock_ret = mock.MagicMock()
        mock_ret.stdout = '\n'.join([
            '             JOBID            PARTITION       NAME ST',
            f'            123456               queue1 {task_key}  R'
        ])
        sysrun_ret = [mock_ret]
        # Mock first call to sysrun -- sbatch (N.B. need one fewer).
        for i in range(len(self.remake.task_ctrl.rescan_tasks),
                       len(self.remake.tasks) - 1):
            mock_ret = mock.MagicMock()
            mock_ret.stdout = f'Submitted batch job {i + 100000}'
            sysrun_ret.append(mock_ret)
        mock_sysrun.side_effect = sysrun_ret
        self.remake.task_ctrl.set_executor('slurm')
        self.remake.run_all()

        # Check that SlurmExecutor._task_already_queued_running was called with correct task.
        mock_already.assert_called_with(running_task)


class TestSlurmExecutorRunJob(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')

    def tearDown(self) -> None:
        sysrun('make reset')
        os.chdir(self.orig_cwd)

    def test_slurm_executor_run_job_rescan(self):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()
        remakefile_hash = sha1(remakefile.read_bytes()).hexdigest()
        rescan_task = remake.task_ctrl.rescan_tasks[0]
        run_job(remakefile, remakefile_hash, 'rescan', rescan_task.filepath)

    def test_slurm_executor_run_job_task1(self):
        remakefile = Path('ex1.py')
        remake = load_remake(remakefile)
        remake.finalize()
        remakefile_hash = sha1(remakefile.read_bytes()).hexdigest()
        rescan_task = remake.task_ctrl.rescan_tasks[0]
        run_job(remakefile, remakefile_hash, 'rescan', rescan_task.filepath)
        task = list(remake.task_ctrl.sorted_tasks.keys())[0]
        run_job(remakefile, remakefile_hash, 'task', task.path_hash_key())
