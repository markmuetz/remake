from pathlib import Path
import traceback

from loguru import logger


def tmp_atomic_path(p):
    p = Path(p)
    return p.parent / ('.remake.tmp.' + p.name)


class Rule:
    @classmethod
    def run_tasks(cls):
        for task in cls.tasks:
            cls.run_task(task)

    @classmethod
    def run_task(cls, task):
        rule = cls()
        rule.logger = logger
        rule.inputs = task.inputs.copy()

        tmp_outputs = {k: tmp_atomic_path(v) for k, v in task.outputs.items()}
        rule.outputs = tmp_outputs
        for output_dir in set(Path(o).parent for o in task.outputs.values()):
            if not output_dir.exists():
                output_dir.mkdir(exist_ok=True, parents=True)

        for k, v in task.kwargs.items():
            setattr(rule, k, v)
        logger.debug(f'Run task: {task}')
        try:
            rule.rule_run()
            task.last_run_status = 1
        except:
            e = traceback.format_exc()
            # task.last_run
            # Set task state to failed.
            logger.error(f'failed: {task}')
            logger.error(f'failed: {e}')
            task.last_run_status = 2
            logger.debug(f'update task: {task}')
            cls.remake.update_task(task, exception=str(e))
            logger.debug(f'updated task: {task}')
            raise
        logger.debug(f'Completed: {task}')

        for output in tmp_outputs.values():
            if not output.exists():
                raise Exception(f'{task}: {output} not created')
        for tmp_path, output_path in zip(tmp_outputs.values(), task.outputs.values()):
            tmp_path.rename(output_path)

        task.is_run = True
        task.requires_rerun = False
        logger.debug(f'update task: {task}')
        cls.remake.update_task(task)
        logger.debug(f'updated task: {task}')

