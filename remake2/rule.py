class Rule:
    @classmethod
    def run_tasks(cls):
        for task in cls.tasks:
            rule = cls()
            rule.inputs = task.inputs.copy()
            rule.outputs = task.outputs.copy()
            for k, v in task.kwargs.items():
                setattr(rule, k, v)
            rule.rule_run()

    @classmethod
    def run_task(cls, task):
        rule = cls()
        rule.inputs = task.inputs.copy()
        rule.outputs = task.outputs.copy()
        for k, v in task.kwargs.items():
            setattr(rule, k, v)
        rule.rule_run()


