import math

from loguru import logger

from ..core.planner import record_failure, upstream_failed
from .executor import Executor


class SingleprocExecutor(Executor):
    def run_tasks(self, tasks):
        ntasks = len(tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1 if ntasks else 1
        nfailed = 0
        nskipped = 0
        failures = {}  # see planner.record_failure
        for i, task in enumerate(tasks):
            prefix = f'{i + 1:>{ndigits}}/{ntasks}'
            if upstream_failed(task, failures):
                # Don't run tasks whose upstream failed this run — they'd
                # fail noisily on missing inputs. Left unrecorded (pending):
                # fixing the upstream makes the next run pick them up.
                # Counts as a failure for downstream propagation.
                record_failure(failures, task)
                nskipped += 1
                logger.warning(f'{prefix} skipped (upstream failed): {task}')
                continue
            logger.info(f'{prefix}: {task}')
            try:
                self.rmk.run_task(task)
            except Exception:
                if self.raise_on_failure:
                    raise  # remake run -X: let the debugger see it
                # Failure is recorded by run_task; carry on so independent
                # tasks still run.
                record_failure(failures, task)
                nfailed += 1
        if nfailed:
            skipped = f' ({nskipped} downstream task(s) skipped)' if nskipped else ''
            logger.error(f'{nfailed}/{ntasks} tasks failed{skipped}')
        return nfailed
