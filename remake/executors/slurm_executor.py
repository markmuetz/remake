import os
import re
import subprocess as sp
from hashlib import sha1
from pathlib import Path

from loguru import logger

from remake.util import sysrun

from .executor import Executor
from ..archive import ArchiveTask


SLURM_SCRIPT_TPL = """#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH -o {task_slurm_output}/task_%j.out
#SBATCH -e {task_slurm_output}/task_%j.err
#SBATCH --comment "{comment}"
{extra_opts}
{dependencies}

echo "SLURM RUNNING {task_key}"
{remake_cmd}
echo "SLURM COMPLETED {task_key}"
"""

REMAKE_RUN_CMD_TPL = """
cd {script_dir}
remake -D run-tasks {remakefile_path} --remakefile-sha1 {remakefile_path_hash} --tasks {task_key}
"""

REMAKE_ARCHIVE_CMD_TPL = """
cd {script_dir}
remake -D archive {archive_file}
"""


def _parse_slurm_jobid(output):
    match = re.match(r'Submitted batch job (?P<jobid>\d+)', output)
    if match:
        jobid = match['jobid']
        return jobid
    else:
        raise Exception(f'Could not parse {output}')


def _submit_slurm_script(slurm_script_path):
    try:
        comp_proc = sysrun(f'sbatch {slurm_script_path}')
        output = comp_proc.stdout
        logger.trace(output.strip())
    except sp.CalledProcessError as cpe:
        logger.error(f'Error submitting {slurm_script_path}')
        logger.error(cpe)
        logger.error('===ERROR===')
        logger.error(cpe.stderr)
        logger.error('===ERROR===')
        raise
    return output


class SlurmExecutor(Executor):
    def __init__(self, rmk, slurm_config=None):
        if not slurm_config:
            slurm_config = {}
        super().__init__(rmk)
        self.rmk = rmk
        default_slurm_kwargs = {'partition': 'short-serial', 'time': '4:00:00', 'mem': 50000}
        slurm_kwargs = {**default_slurm_kwargs}
        slurm_kwargs.update(slurm_config)
        logger.trace(slurm_kwargs)

        self.slurm_dir = Path('.remake/slurm/scripts')
        self.slurm_dir.mkdir(exist_ok=True, parents=True)
        self.slurm_output = Path('.remake/slurm/output')
        self.slurm_output.mkdir(exist_ok=True, parents=True)

        self.remakefile_path = Path(rmk.name)
        self.slurm_kwargs = slurm_kwargs
        self.task_jobid_map = {}
        self.remakefile_path_hash = sha1(self.remakefile_path.read_bytes()).hexdigest()
        self.check_slurm_tasks()

    def check_slurm_tasks(self):
        # Check to see whether this task is already running.
        username = os.getlogin()

        try:
            # get jobid, partition and job name.
            # job name is 10 character task key.
            output = sysrun(f'squeue -u {username} -o "%.18i %.20P %.10j %.3t"').stdout
            logger.trace(output.strip())
        except sp.CalledProcessError as cpe:
            logger.error('Error on squeue command')
            logger.error(cpe)
            logger.error('===ERROR===')
            logger.error(cpe.stderr)
            logger.error('===ERROR===')
            raise
        # Parse output. Skip first and blank lines.
        self.currently_running_task_keys = {}
        for line in output.split('\n')[1:]:
            if not line:
                continue
            jobid, partition, task_key, status = line.split()
            if status in ['PD', 'R']:
                self.currently_running_task_keys[task_key] = {
                    'jobid': jobid,
                    'partition': partition,
                }

    def run_tasks(
        self, rerun_tasks, show_reasons=False, show_task_code_diff=False, stdout_to_log=False
    ):
        for task in rerun_tasks:
            self._submit_task(task, show_reasons, show_task_code_diff)

    def _write_submit_script(self, task):
        remakefile_name = self.remakefile_path.stem
        # script_path = Path(__file__)
        rule_name = task.rule.__name__
        rule_slurm_output = self.slurm_output / rule_name
        task_key = task.key()
        if hasattr(task.rule, 'var_matrix') or hasattr(task.rule, 'rule_matrix'):
            task_dir = [task_key[:2], task_key[2:]]
            # Doesn't work if val is e.g. a datetime.
            # task_dir = [f'{k}-{getattr(task, k)}' for k in task.var_matrix.keys()]
            task_slurm_output = rule_slurm_output.joinpath(*task_dir)
        else:
            task_slurm_output = rule_slurm_output

        slurm_kwargs = {**self.slurm_kwargs}
        rule_config = getattr(task.rule, 'config', {})
        if 'slurm' in rule_config:
            logger.debug(f'  updating {task} config: {rule_config["slurm"]}')
            slurm_kwargs.update(rule_config['slurm'])

        logger.trace(f'  creating {task_slurm_output}')
        task_slurm_output.mkdir(exist_ok=True, parents=True)
        slurm_script_filepath = self.slurm_dir.joinpath(
            *[task_key[:2], task_key[2:], f'{remakefile_name}_{task.key()}.sbatch']
        )
        slurm_script_filepath.parent.mkdir(exist_ok=True, parents=True)

        prev_jobids = []
        for prev_task in task.prev_tasks:
            # N.B. not all dependencies have to have been run; they could not require rerunning.
            if prev_task in self.task_jobid_map:
                prev_jobids.append(self.task_jobid_map[prev_task])
        if prev_jobids:
            dependencies = '#SBATCH --dependency=afterok:' + ':'.join(prev_jobids)
        else:
            dependencies = ''

        extra_opts = []
        for k, v in slurm_kwargs.items():
            if not v:
                continue
            if k == 'max_runtime':
                extra_opts.append(f'#SBATCH --time={v}')
            elif k == 'queue':
                extra_opts.append(f'#SBATCH --partition={v}')
            else:
                extra_opts.append(f'#SBATCH --{k}={v}')
        comment = str(task)
        extra_opts = '\n'.join(extra_opts)
        if isinstance(task, ArchiveTask):
            remake_cmd = REMAKE_ARCHIVE_CMD_TPL.format(
                script_dir=Path.cwd(), archive_file=task.archive_file
            )
        else:
            # cd {script_dir}
            # remake -D run-tasks {remakefile_path} --remakefile-sha1 {remakefile_path_hash} --tasks {task_key}
            remake_cmd = REMAKE_RUN_CMD_TPL.format(
                script_dir=Path.cwd(),
                remakefile_path=self.remakefile_path,
                remakefile_path_hash=self.remakefile_path_hash,
                task_key=task_key,
            )
        slurm_script = SLURM_SCRIPT_TPL.format(
            # script_dir=Path.cwd(),
            task_slurm_output=task_slurm_output,
            comment=comment,
            remakefile_name=remakefile_name,
            # remakefile_path=self.remakefile_path,
            # remakefile_path_hash=self.remakefile_path_hash,
            task_key=task_key,
            extra_opts=extra_opts,
            dependencies=dependencies,
            job_name=task_key[:10],  # Longer and a leading * is added.
            remake_cmd=remake_cmd,
            **slurm_kwargs,
        )

        logger.trace(f'  writing {slurm_script_filepath}')
        logger.trace('\n' + slurm_script)
        with open(slurm_script_filepath, 'w') as fp:
            fp.write(slurm_script)
        return slurm_script_filepath, slurm_kwargs['partition']

    def _submit_task(self, task, show_reasons, show_task_code_diff):
        diffs = {}
        task_key = task.key()
        # Make sure task isn't already queued or running.
        if task_key[:10] in self.currently_running_task_keys:
            partition = self.currently_running_task_keys[task_key[:10]]['partition']
            logger.info(f'Already queued/running [{partition}]: {task}')
            jobid = self.currently_running_task_keys[task_key[:10]]['jobid']
        else:
            # N.B. you HAVE to write then submit, because you need to job ids for deps.
            slurm_script_path, partition = self._write_submit_script(task)
            output = _submit_slurm_script(slurm_script_path)
            logger.info(f'Submitted [{partition}]: {task}')
            if show_reasons:
                self.rmk.show_task_reasons(task)
            if show_task_code_diff:
                diffs = self.rmk.show_task_code_diff(task, diffs)
            jobid = _parse_slurm_jobid(output)
        self.task_jobid_map[task] = jobid
