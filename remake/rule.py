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
        tmp_outputs = {k: tmp_atomic_path(v) for k, v in task.outputs.items()}
        for output_dir in set(Path(o).parent for o in task.outputs.values()):
            if not output_dir.exists():
                output_dir.mkdir(exist_ok=True, parents=True)

        logger.debug(f'Run task: {task}')
        try:
            if cls.remake.config['old_style_class']:
                rule = cls()
                rule.logger = logger
                rule.inputs = task.inputs.copy()
                rule.outputs = tmp_outputs
                for k, v in task.kwargs.items():
                    setattr(rule, k, v)
                rule.rule_run()
            else:
                cls.rule_run(task.inputs, tmp_outputs, **task.kwargs)
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

