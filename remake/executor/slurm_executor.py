import os
import re
import sys
from hashlib import sha1
import logging
import subprocess as sp
from pathlib import Path

from remake.util import sysrun
from remake.setup_logging import setup_stdout_logging
from remake.load_task_ctrls import load_task_ctrls
from remake.task import Task, RescanFileTask


SLURM_SCRIPT_TPL = """#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH -p {queue}
#SBATCH -o slurm_output/{script_name}_{remakefile_name}_{task_type}_%j.out
#SBATCH -e slurm_output/{script_name}_{remakefile_name}_{task_type}_%j.err
#SBATCH --time={max_runtime}
#SBATCH --mem={mem}
{dependencies}

python {script_path} {remakefile_path} {remakefile_path_hash} {task_key}
"""

logging.basicConfig(stream=sys.stdout, level=os.getenv('REMAKE_LOGLEVEL', 'INFO'),
                    format='%(asctime)s %(levelname)8s: %(message)s')
logger = logging.getLogger(__name__)


def _parse_jobid(output):
    if match := re.match('Submitted batch job (?P<jobid>\d+)', output):
        jobid = match['jobid']
        return jobid
    else:
        raise Exception(f'Could not parse {output}')


def _submit_slurm_script(slurm_script_path):
    try:
        output = slurm_script_path.read_text()
        # comp_proc = sysrun(f'sbatch {slurm_script_path}')
        # output = comp_proc.stdout
        logger.info(output)
    except sp.CalledProcessError as cpe:
        logger.error(f'Error submitting {slurm_script_path}')
        logger.error(cpe)
        logger.error('===ERROR===')
        logger.error(cpe.stderr)
        logger.error('===ERROR===')
        raise
    return output


class SlurmExecutor:
    handles_dependencies = True

    def __init__(self, task_ctrl):
        slurm_kwargs = {'queue': 'short-serial',
                        'max_runtime': '04:00:00',
                        'mem': 16000}

        slurm_dir = Path('slurm_scripts')
        slurm_dir.mkdir(exist_ok=True)
        slurm_output = Path('slurm_output')
        slurm_output.mkdir(exist_ok=True)

        self.slurm_dir = slurm_dir
        self.remakefile_path = Path(task_ctrl.name + '.py').absolute()
        self.task_ctrl = task_ctrl
        self.slurm_kwargs = slurm_kwargs
        self.task_jobid_map = {}
        self.remakefile_path_hash = sha1(self.remakefile_path.read_bytes()).hexdigest()
        self.pending_tasks = []

    def _write_submit_script(self, task):
        remakefile_name = self.remakefile_path.stem
        script_path = Path(__file__)
        script_name = script_path.stem
        slurm_script_filepath = self.slurm_dir / f'{script_name}_{remakefile_name}_{task.path_hash_key()}.sbatch'
        logger.debug(f'  writing {slurm_script_filepath}')

        prev_jobids = []
        prev_tasks = self.task_ctrl.task_dag.predecessors(task)
        for prev_task in prev_tasks:
            # N.B. not all dependencies have to have been run; they could not require rerunning.
            if prev_task in self.task_jobid_map:
                prev_jobids.append(self.task_jobid_map[prev_task])
        if prev_jobids:
            dependencies = '#SBATCH --dependency=afterok:' + ':'.join(prev_jobids)
        else:
            dependencies = ''

        if isinstance(task, Task):
            task_type = 'task'
            task_key = task.path_hash_key(),
        elif isinstance(task, RescanFileTask):
            task_type = 'rescan'
            task_key = str(task.inputs['filepath'])
        else:
            raise ValueError(f'Unkown task type: {task}')

        slurm_script = SLURM_SCRIPT_TPL.format(script_name=script_name,
                                               script_path=script_path,
                                               remakefile_name=remakefile_name,
                                               remakefile_path=self.remakefile_path,
                                               remakefile_path_hash=self.remakefile_path_hash,
                                               task_type=task_type,
                                               task_key=task_key,
                                               dependencies=dependencies,
                                               job_name=task_key[:10],  # Longer and a leading * is added.
                                               **self.slurm_kwargs)

        with open(slurm_script_filepath, 'w') as fp:
            fp.write(slurm_script)
        return slurm_script_filepath

    def _submit_task(self, task):
        slurm_script_path = self._write_submit_script(task)
        output = _submit_slurm_script(slurm_script_path)
        jobid = _parse_jobid(output)
        self.task_jobid_map[task] = jobid

    def can_accept_task(self):
        return True

    def enqueue_task(self, task):
        self.pending_tasks.append(task)

    def get_completed_task(self):
        raise NotImplementedError('Should not be called for SlurmExecutor')

    def finish(self):
        for task in self.pending_tasks:
            self._submit_task(task)


def main(remakefile, remakefile_hash, task_type, task_key):
    setup_stdout_logging('DEBUG')

    remakefile = Path(remakefile).absolute()
    curr_remakefile_hash = sha1(remakefile.read_bytes()).hexdigest()
    if remakefile_hash != curr_remakefile_hash:
        raise Exception(f'config file {remakefile} has changed -- cannot run task.')

    task_ctrl = load_task_ctrls(remakefile)[0]
    assert not task_ctrl.finalized, f'task control {task_ctrl} already finalized'
    # Note, task_ctrl is not finalized.
    # This is because another task could be finishing, and writing its output's metadata
    # when this is called, and finalize can be trying to read it at the same time.
    # Can perhaps fix if instead Task is responsible for working out if rerun needed,
    # and removing finalize here.
    if task_type == 'task':
        task = task_ctrl.task_from_path_hash_key[task_key]
    elif task_type == 'rescan':
        task = task_ctrl.gen_rescan_task(task_key)
    task_ctrl.run_task(task)


if __name__ == '__main__':
    print(sys.argv)
    main(*sys.argv[1:])


