import abc


class Executor(abc.ABC):
    # True: Remake.run hands over the whole plan in one call —
    # run_tasks(tasks, deferred_rules) — instead of driving the replanning
    # loop. For executors that submit asynchronously (SLURM), deferral is
    # handled by a continuation job, not by replanning in this process.
    handles_deferred = False
    # True: `remake run --dry-run` sets executor.dry_run and still calls
    # run_tasks (generate everything, submit nothing).
    supports_dry_run = False

    def __init__(self, rmk):
        self.rmk = rmk

    @abc.abstractmethod
    def run_tasks(self, tasks):
        pass
