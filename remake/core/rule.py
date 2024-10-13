from pathlib import Path
import traceback

from loguru import logger

from .exceptions import RemakeOutputNotCreated


def tmp_atomic_path(p):
    p = Path(p)
    return p.parent / ('.remake.tmp.' + p.name)


class Rule:
    @classmethod
    def run_task(cls, task, save_status=True):
        assert task.rule == cls, f'Task has wrong rule: {task.rule} != {cls}'
        tmp_outputs = {k: tmp_atomic_path(v) for k, v in task.outputs.items()}
        for output_dir in set(Path(o).parent for o in task.outputs.values()):
            if not output_dir.exists():
                output_dir.mkdir(exist_ok=True, parents=True)

        logger.debug(f'Run task: {task}')
        try:
            cls.rule_run(task.inputs, tmp_outputs, **task.kwargs)
            for output in tmp_outputs.values():
                if not output.exists():
                    # TODO:
                    # This should really have a different fail status.
                    # Reason being: if this happens, then no amount of rerunning will fix it.
                    raise RemakeOutputNotCreated(f'{task}: {output} not created')
            for tmp_path, output_path in zip(tmp_outputs.values(), task.outputs.values()):
                tmp_path.rename(output_path)
        except:
            e = traceback.format_exc()
            # Set task state to failed.
            logger.error(f'failed: {task}')
            logger.error(f'failed: {e}')
            task.last_run_status = 2
            if save_status:
                logger.debug(f'update task: {task}')
                cls.remake.update_task(task, exception=str(e))
                logger.debug(f'updated task: {task}')
            raise

        logger.debug(f'Completed: {task}')
        task.last_run_status = 1
        task.is_run = True
        task.requires_rerun = False

        if save_status:
            logger.debug(f'update task: {task}')
            cls.remake.update_task(task)
            logger.debug(f'updated task: {task}')
