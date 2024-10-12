from pathlib import Path

from loguru import logger

from ..util import CodeComparer


def _compare_task_timestamps(t1, t2):
    if t1 is None or t2 is None:
        return False
    else:
        return t1 < t2


class TaskControl:
    def __init__(self):
        self.code_comparer = CodeComparer()

    def set_task_statuses(self, tasks, remake_outputs, config):
        for task in tasks:
            logger.trace(f'set status for {task}')
            if hasattr(task.rule, 'config'):
                config = config.copy()
                config.update(str(task), task.rule.config)

            requires_rerun = False
            rerun_reasons = []
            if task.last_run_status == 0:
                requires_rerun = True
                logger.trace('task_not_run')
                rerun_reasons.append('task_not_run')
            elif task.last_run_status == 2:
                # Note, we do not know *why* the task failed.
                # It could have nothing to do with the Python code, e.g. out of memory/time etc.
                # Mark as requires_rerun so that if these have been fixed, the task can be rerun.
                requires_rerun = True
                logger.trace('task_failed')
                rerun_reasons.append('task_failed')
            for prev_task in task.prev_tasks:
                if prev_task.inputs_missing:
                    task.inputs_missing = True
                    rerun_reasons.append(f'prev_task_input_missing {prev_task}')
                    logger.trace(f'prev_task_input_missing {prev_task}')
                    requires_rerun = False
                if prev_task.requires_rerun and not prev_task.inputs_missing:
                    requires_rerun = True
                    logger.trace(f'prev_task_requires_rerun {prev_task}')
                    rerun_reasons.append(f'prev_task_requires_rerun {prev_task}')
                if _compare_task_timestamps(task.last_run_timestamp, prev_task.last_run_timestamp):
                    requires_rerun = True
                    logger.trace(f'prev_task_run_more_recently {prev_task}')
                    rerun_reasons.append(f'prev_task_run_more_recently {prev_task}')

            earliest_output_path_mtime = float('inf')
            latest_input_path_mtime = 0
            if config['check_outputs_older_than_inputs'] or config['check_inputs_exist']:
                # TODO: Cache files to avoid hitting FS more than once per file.
                all_inputs_present = True
                for path in task.inputs.values():
                    if not Path(path).exists():
                        if path in self.remake_outputs and self.remake_outputs[path].requires_rerun:
                            pass
                        else:
                            requires_rerun = False
                            logger.trace(f'input_missing {path}')
                            rerun_reasons.append(f'input_missing {path}')
                            task.inputs_missing = True
                        all_inputs_present = False
                    else:
                        latest_input_path_mtime = max(
                            latest_input_path_mtime, Path(path).lstat().st_mtime
                        )
                if all_inputs_present:
                    logger.trace('all_inputs_present')
                    task.inputs_missing = False
                    rerun_reasons = [
                        r for r in rerun_reasons if not r.startswith('prev_task_input_missing')
                    ]

            if config['check_outputs_older_than_inputs'] or config['check_outputs_exist']:
                for path in task.outputs.values():
                    if not Path(path).exists():
                        requires_rerun = True
                        logger.trace(f'output_missing {path}')
                        rerun_reasons.append(f'output_missing {path}')
                    else:
                        earliest_output_path_mtime = min(
                            earliest_output_path_mtime, Path(path).lstat().st_mtime
                        )

            if config['check_outputs_older_than_inputs']:
                if latest_input_path_mtime > earliest_output_path_mtime:
                    requires_rerun = True
                    logger.trace('input_is_older_than_output')
                    rerun_reasons.append('input_is_older_than_output')

            if not self.code_comparer(
                task.last_run_code, task.rule.source['rule_run']
            ):
                if not task.inputs_missing:
                    requires_rerun = True
                logger.trace('task_run_source_changed')
                rerun_reasons.append('task_run_source_changed')

            task.requires_rerun = requires_rerun
            task.rerun_reasons = rerun_reasons
            logger.debug(f'R={task.requires_rerun}, M={task.inputs_missing}: {task}')
            # if task.rule.__name__ == 'RegridImergToN216':
            #     raise Exception()


