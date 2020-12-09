class TaskQuerySet(list):
    def __init__(self, iterable=None, task_ctrl=None):
        self.task_ctrl = task_ctrl
        if not iterable:
            iterable = []
        super().__init__(iterable)

    def all(self):
        return self

    def filter(self, cast_to_str=False, **kwargs):
        return TaskQuerySet(self._filter(cast_to_str, **kwargs), task_ctrl=self.task_ctrl)

    def _filter(self, cast_to_str=False, **kwargs):
        for task in self:
            for k, v in kwargs.items():
                if cast_to_str:
                    if str(getattr(task, k, None)) == str(v):
                        yield task
                else:
                    if getattr(task, k, None) == v:
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
        self.task_ctrl.run(requested_tasks=self, force=force)
