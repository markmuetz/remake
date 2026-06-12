import math

from loguru import logger

from .executor import Executor


class SingleprocExecutor(Executor):
    def run_tasks(self, tasks):
        ntasks = len(tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1 if ntasks else 1
        nfailed = 0
        for i, task in enumerate(tasks):
            logger.info(f'{i + 1:>{ndigits}}/{ntasks}: {task}')
            try:
                self.rmk.run_task(task)
            except Exception:
                # Failure is recorded by run_task; carry on so independent
                # tasks still run. Downstream tasks of a failure will fail
                # on their missing inputs and be recorded too.
                nfailed += 1
        if nfailed:
            logger.error(f'{nfailed}/{ntasks} tasks failed')
        return nfailed
