import math

from loguru import logger

from .executor import Executor


class SingleprocExecutor(Executor):
    def run_tasks(self, rerun_tasks):
        ntasks = len(rerun_tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1
        for i, task in enumerate(rerun_tasks):
            logger.info(f'{i + 1:>{ndigits}}/{ntasks}: {task}')
            task.run()



