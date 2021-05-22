class TaskQuerySet(list):
    def __init__(self, iterable=None, task_ctrl=None):
        self.task_ctrl = task_ctrl
        if not iterable:
            iterable = []
        super().__init__(iterable)

    def __getitem__(self, i):
        # Returns a TaskQuerySet if a slice is used, else an individual task.
        if isinstance(i, slice):
            return TaskQuerySet(list.__getitem__(self, i), self.task_ctrl)
        else:
            return list.__getitem__(self, i)

    def all(self):
        return self

    def in_rule(self, rule):
        if isinstance(rule, str):
            return TaskQuerySet([t for t in self if t.__class__.__name__ == rule], self.task_ctrl)
        else:
            return TaskQuerySet([t for t in self if t.__class__ is rule], self.task_ctrl)

    def filter(self, cast_to_str=False, **kwargs):
        return TaskQuerySet(self._filter(cast_to_str, **kwargs), task_ctrl=self.task_ctrl)

    def _filter(self, cast_to_str=False, **kwargs):
        for task in self:
            has_all_vals = True
            for k, v in kwargs.items():
                if cast_to_str:
                    if str(getattr(task, k, None)) != str(v):
                        has_all_vals = False
                        break
                else:
                    if getattr(task, k, None) != v:
                        has_all_vals = False
                        break
            if has_all_vals:
                yield task

    def filter_on_inputs(self, inputs):
        return TaskQuerySet(self._filter_on_inputs(inputs), task_ctrl=self.task_ctrl)

    def _filter_on_inputs(self, inputs):
        for task in self:
            for i in inputs:
                if i in task.inputs:
                    yield task

    def filter_on_outputs(self, outputs):
        return TaskQuerySet(self._filter_on_outputs(outputs), task_ctrl=self.task_ctrl)

    def _filter_on_outputs(self, outputs):
        for task in self:
            for i in outputs:
                if i in task.outputs:
                    yield task

    def exclude(self, **kwargs):
        return TaskQuerySet(self._exclude(**kwargs), task_ctrl=self.task_ctrl)

    def _exclude(self, **kwargs):
        for task in self:
            for k, v in kwargs.items():
                if getattr(task, k, None) != v:
                    yield task

    def get(self, **kwargs):
        task_iter = self._filter(**kwargs)
        try:
            task = next(task_iter)
        except StopIteration:
            raise Exception(f'No task found matching {kwargs}')
        try:
            next(task_iter)
            raise Exception(f'More than one task found matching {kwargs}')
        except StopIteration:
            return task

    def first(self):
        if not self:
            raise Exception('No task found')
        return self[0]

    def last(self):
        if not self:
            raise Exception('No task found')
        return self[-1]

    def run(self, force=False):
        self.task_ctrl.run_requested(requested_tasks=self, force=force)

    def status(self, reasons=False, task_diff=False):
        for task in self:
            print(f'{task.path_hash_key()[:6]}: {task.status:<10} - {task}')
            if reasons:
                for reason in task.task_md.rerun_reasons:
                    if reason[1]:
                        print(f'  {reason[0]}: {reason[1]}')
                    else:
                        print(f'  {reason[0]}')
            if task_diff:
                if task_diff := task.diff():
                    print('\n'.join(task_diff))

