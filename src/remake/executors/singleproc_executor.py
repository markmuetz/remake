import math

from loguru import logger

from ..core.planner import upstream_failed
from .executor import Executor


class SingleprocExecutor(Executor):
    def run_tasks(self, tasks):
        ntasks = len(tasks)
        ndigits = math.floor(math.log10(ntasks)) + 1 if ntasks else 1
        nfailed = 0
        nskipped = 0
        failures = {}  # rule -> set of frozenset(kwargs.items())
        for i, task in enumerate(tasks):
            prefix = f'{i + 1:>{ndigits}}/{ntasks}'
            if upstream_failed(task, failures):
                # Don't run tasks whose upstream failed this run — they'd
                # fail noisily on missing inputs. Left unrecorded (pending):
                # fixing the upstream makes the next run pick them up.
                # Counts as a failure for downstream propagation.
                failures.setdefault(task.rule, set()).add(frozenset(task.kwargs.items()))
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
                failures.setdefault(task.rule, set()).add(frozenset(task.kwargs.items()))
                nfailed += 1
        if nfailed:
            skipped = f' ({nskipped} downstream task(s) skipped)' if nskipped else ''
            logger.error(f'{nfailed}/{ntasks} tasks failed{skipped}')
        return nfailed
