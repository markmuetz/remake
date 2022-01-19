import inspect
import itertools
import multiprocessing
from logging import getLogger

from remake.remake_exceptions import MissingTaskRuleProperty, TaskRuleNameError
from remake.task import Task
from remake.remake_base import Remake
from remake.task_query_set import TaskQuerySet
from remake.util import format_path

logger = getLogger(__name__)


class RemakeMetaclass(type):
    required_properties = [
        'rule_inputs',
        'rule_outputs',
        'rule_run',
    ]
    """Provides the machinery for actually creating `TaskRule` classes.

    Uses the information provided by a `TaskRule` to create instances of the task rule, and add them the
    `TaskRule` .tasks list."""
    def __new__(mcs, clsname, bases, attrs):
        depends_on = []
        if clsname not in ['TaskRule']:
            # Do not do anything for TaskRule class creation below.
            remake = Remake.current_remake[multiprocessing.current_process().name]
            if 'TaskRule' in [b.__name__ for b in bases]:
                logger.debug(f'creating TaskRule-derived class {clsname}')
                # Only apply to subclasses of TaskRule.
                for prop in RemakeMetaclass.required_properties:
                    if prop not in attrs:
                        raise MissingTaskRuleProperty(f'TaskRule requires property `{prop}` to be set')

                # All public methods are treated as being depends_on functions.
                for key, attr in attrs.items():
                    if key[0] == '_' or key == 'rule_run':
                        continue
                    # N.B. method has not been bound, so it appears like a function.
                    if inspect.isfunction(attr):
                        depends_on.append(attr)
                attrs['tasks'] = TaskQuerySet(task_ctrl=remake.task_ctrl)
                attrs['task_ctrl'] = remake.task_ctrl
                attrs['next_rules'] = set()
                attrs['prev_rules'] = set()

        newcls = super(RemakeMetaclass, mcs).__new__(
            mcs, clsname, bases, attrs)

        if not attrs.get('enabled', True):
            logger.debug(f'  {clsname} is disabled')
            return newcls

        if clsname not in ['TaskRule']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                if hasattr(remake, newcls.__name__):
                    raise TaskRuleNameError(f'Task rule name clashes with existing method/property:'
                                            f' {newcls.__name__}')
                setattr(remake, newcls.__name__, newcls)
                remake.rules.append(newcls)
                var_matrix = attrs.get('var_matrix', None)
                depends_on.extend(attrs.get('depends_on', []))
                logger.debug(f'  depends on: {depends_on}')
                if var_matrix:
                    all_loop_vars = list(itertools.product(*var_matrix.values()))
                    logger.debug(f'  creating {len(all_loop_vars)} instances of {clsname}')

                    for loop_vars in all_loop_vars:
                        # e.g. var_matrix = {'a': [1, 2], 'b': [3, 4]}
                        # run for [(1, 3), (1, 4), (2, 3), (2, 4)].
                        fmt_dict = {k: v for k, v in zip(var_matrix.keys(), loop_vars)}
                        fmt_dict = RemakeMetaclass._check_modify_fmt_dict(fmt_dict)
                        # e.g. for (1, 3): fmt_dict = {'a': 1, 'b': 3}
                        inputs = RemakeMetaclass._create_inputs_ouputs(attrs['rule_inputs'], fmt_dict)
                        outputs = RemakeMetaclass._create_inputs_ouputs(attrs['rule_outputs'], fmt_dict)
                        # Creates an instance of the class. N.B. TaskRule inherits from Task, so Task.__init__ is
                        # called here.
                        task = newcls(remake.task_ctrl, attrs['rule_run'], inputs, outputs,
                                      depends_on=depends_on)
                        # Set up the instance variables so that e.g. within TaskRule.rule_run, self.a == 1.
                        for k, v in zip(var_matrix.keys(), loop_vars):
                            if isinstance(k, tuple):
                                if not isinstance(v, tuple) or len(k) != len(v):
                                    raise Exception(f'{k} and {v} do not match')
                                for kk, vv in zip(k, v):
                                    setattr(task, kk, vv)
                            else:
                                setattr(task, k, v)
                        newcls.tasks.append(task)
                        remake.task_ctrl.add(task)
                else:
                    logger.debug(f'  creating instance of {clsname}')
                    inputs = RemakeMetaclass._create_inputs_ouputs(attrs['rule_inputs'], {})
                    outputs = RemakeMetaclass._create_inputs_ouputs(attrs['rule_outputs'], {})
                    task = newcls(remake.task_ctrl, attrs['rule_run'],
                                  inputs, outputs,
                                  depends_on=depends_on)
                    newcls.tasks.append(task)
                    remake.task_ctrl.add(task)

                remake.tasks.extend(newcls.tasks)
        return newcls

    @staticmethod
    def _check_modify_fmt_dict(fmt_dict):
        errors = []
        new_fmt_dict = {**fmt_dict}
        for key, value in fmt_dict.items():
            if isinstance(key, str):
                continue
            elif isinstance(key, tuple):
                if not len(key) == len(value):
                    errors.append(('length mismatch', key, value))
                for kk, vv in zip(key, value):
                    if not isinstance(kk, str):
                        errors.append(('not tuple of strings', key, value))
                        break
                    else:
                        new_fmt_dict[kk] = vv
                new_fmt_dict.pop(key)
            else:
                errors.append(('not string', key, value))
        if errors:
            error_str = '\n'.join([f'  {msg}: {k}, {v}' for msg, k, v in errors])
            raise Exception(f'input_rule/output_rule keys must be strings or tuples of strings:\n'
                            f'{error_str}')
        return new_fmt_dict

    @staticmethod
    def _create_inputs_ouputs(rule_inputs_outputs, fmt_dict):
        # This is a little gnarly.
        # See: https://stackoverflow.com/questions/41921255/staticmethod-object-is-not-callable
        # Method has not been bound yet, but you can call it using its __func__ attr.
        # N.B. both are possible, if e.g. a second rule uses a first rule's method.
        if hasattr(rule_inputs_outputs, '__func__'):
            return rule_inputs_outputs.__func__(**fmt_dict)
        elif callable(rule_inputs_outputs):
            return rule_inputs_outputs(**fmt_dict)
        else:
            return {k.format(**fmt_dict): format_path(v, **fmt_dict)
                    for k, v in rule_inputs_outputs.items()}


class TaskRule(Task, metaclass=RemakeMetaclass):
    """Core class. Defines a set of tasks in a remakefile.

    Each class must have class-level properties: rule_inputs, rule_outputs, and each must have a method: rule_run.
    Each output file must be unique within a remakefile.
    In the rule_run method, the inputs and outputs are available through e.g. the self.inputs property.

    >>> demo = Remake()
    >>> class TaskSet(TaskRule):
    ...     rule_inputs = {'in': 'infile'}
    ...     rule_outputs = {'out': 'outfile'}
    ...     def rule_run(self):
    ...         self.outputs['out'].write_text(self.inputs['in'].read_text())
    >>> len(TaskSet.tasks)
    1

    Each class can also optionally define a var_matrix, and dependency functions/classes. `var_matrix` should be
    a dict with string keys, and a list of items for each key. There will be as many tasks created as the
    `itertools.product` between the lists for each key. The values will be substituted in to the inputs/outputs.

    >>> def fn():
    ...     print('in fn')
    >>> class TaskSet2(TaskRule):
    ...     rule_inputs = {'in': 'infile'}
    ...     rule_outputs = {'out_{i}{j}': 'outfile_{i}{j}'}
    ...     var_matrix = {'i': [1, 2], 'j': [3, 4]}
    ...     dependencies = [fn]
    ...     def rule_run(self):
    ...         fn()
    ...         self.outputs[f'out_{self.i}'].write_text(str(self.i) + self.inputs['in'].read_text())
    >>> len(TaskSet2.tasks)
    4

    Note, all tasks created by these `TaskRule` are added to the `Remake` object:

    >>> len(demo.tasks)
    5

    When the remakefile is run (`$ remake run` on the command line), all the tasks will be triggered according to their
    ordering. If any of the rule_run methods is changed, then those tasks will be rerun, and if their output is
    is different subsequent tasks will be rerun.
    """
    def __getattr__(self, item):
        """By implementing this, IDEs like PyCharm will not complain when `self.i` used in `rule_run`"""
        if item not in self.__dict__:
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{item}'")
        return self.__dict__[item]


# TODO: Ideas for new TaskRules:
# class CommandTaskRule(TaskRule):
#    command = ...
# class ScriptTaskRule(TaskRule):
#    script = ...
