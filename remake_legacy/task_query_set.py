class TaskQuerySet(list):
    """List of classes with some extra capabilities.

    Provides some simple methods for filtering, running and displaying the status of all its tasks.
    """
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

    def in_rule(self, rule):
        """Filter all tasks to those in provided rule.

        :param rule: rule to filter on
        :return: filtered TaskQuerySet
        """
        if isinstance(rule, str):
            return TaskQuerySet([t for t in self if t.__class__.__name__ == rule], self.task_ctrl)
        else:
            return TaskQuerySet([t for t in self if t.__class__ is rule], self.task_ctrl)

    def filter(self, cast_to_str=False, **kwargs):
        """Filter tasks based on kwargs.

        >>> class DummyTask:
        ...     pass
        >>> tasks = []
        >>> for i in range(10):
        ...     task = DummyTask()
        ...     setattr(task, 'i', i)
        ...     setattr(task, 'j', i % 3 == 0)
        ...     tasks.append(task)
        >>> task_query_set = TaskQuerySet(tasks, None)
        >>> len(task_query_set.filter(j=True))
        4

        :param cast_to_str: Cast values to string first
        :param kwargs: key-value pairs of task properties
        :return: filtered TaskQuerySet
        """
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

    def exclude(self, **kwargs):
        """As `filter`, but exclude instead of include tasks.

        :param kwargs: key-value pairs to exclude
        :return: filtered tasks
        """
        return TaskQuerySet(self._exclude(**kwargs), task_ctrl=self.task_ctrl)

    def _exclude(self, **kwargs):
        for task in self:
            for k, v in kwargs.items():
                if getattr(task, k, None) != v:
                    yield task

    def get(self, **kwargs):
        """Get one and only one task.

        :param kwargs: key-value pairs to get
        :return: task meeting criteria
        """
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
        """Get first task."""
        if not self:
            raise Exception('No task found')
        return self[0]

    def last(self):
        """Get last task."""
        if not self:
            raise Exception('No task found')
        return self[-1]

    def run(self, force=False):
        """Run all tasks.

        :param force: force run
        """
        self.task_ctrl.run_requested(requested_tasks=self, force=force)

    def status(self, reasons=False, task_diff=False):
        """Print status for all tasks.

        :param reasons: show reasons for why tasks have their statuses
        :param task_diff: show a diff of the tasks' rule_run methods
        """
        for task in self:
            print(f'{task.path_hash_key()[:6]}: {task.status:<10} - {task}')
            if reasons:
                for reason in task.task_md.rerun_reasons:
                    if reason[1]:
                        print(f'  {reason[0]}: {reason[1]}')
                    else:
                        print(f'  {reason[0]}')
            if task.status == 'cannot_run':
                print('  cannot_run:')
                for k, p in task.inputs.items():
                    if p.exists():
                        continue
                    print(f'    does not exist: {k}, {p}')
            if task_diff:
                task_diff = task.diff()
                if task_diff:
                    print('\n'.join(task_diff))
