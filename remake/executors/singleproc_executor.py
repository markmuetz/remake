import math

from loguru import logger

from ..util import Capturing
from .executor import Executor


class SingleprocExecutor(Executor):
    def run_tasks(
        self, rerun_tasks, show_reasons=False, show_task_code_diff=False, stdout_to_log=False
    ):
        ntasks = len(rerun_tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1
        diffs = {}
        for i, task in enumerate(rerun_tasks):
            logger.info(f'{i + 1:>{ndigits}}/{ntasks}: {task}')
            if show_reasons:
                self.rmk.show_task_reasons(task)
            if show_task_code_diff:
                diffs = self.rmk.show_task_code_diff(task, diffs)

            if stdout_to_log:
                with Capturing() as output:
                    task.run()
                for line in output:
                    logger.debug(line)
            else:
                task.run()
