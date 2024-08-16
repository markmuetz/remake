def tmp_atomic_path(p):
    return p.parent / ('.remake.tmp.' + p.name)


class Rule:
    @classmethod
    def run_tasks(cls):
        for task in cls.tasks:
            cls.run_task(task)

    @classmethod
    def run_task(cls, task):
        rule = cls()
        rule.inputs = task.inputs.copy()

        tmp_outputs = {k: tmp_atomic_path(v) for k, v in task.outputs.items()}
        rule.outputs = tmp_outputs

        for k, v in task.kwargs.items():
            setattr(rule, k, v)
        rule.rule_run()

        for output in tmp_outputs.values():
            if not output.exists():
                raise Exception(f'{task}: {output} not created')
        for tmp_path, output_path in zip(tmp_outputs.values(), task.outputs.values()):
            tmp_path.rename(output_path)

        task.is_run = True
        task.requires_rerun = False
        cls.remake.update_task(task)

