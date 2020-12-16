import itertools
import multiprocessing

from remake.task import Task
from remake.remake_base import Remake
from remake.task_query_set import TaskQuerySet
from remake.util import fmtp


class LoopVar:
    # Not sure if I should use this or original (define a var_matrix)
    def __init__(self, loop):
        self.loop = loop

    def __iter__(self):
        for v in self.loop:
            yield v


class RemakeMetaclass(type):
    def __new__(mcs, clsname, bases, attrs):
        if clsname not in ['TaskRule', 'Config']:
            remake = Remake.current_remake[multiprocessing.current_process().name]
            if 'TaskRule' in [b.__name__ for b in bases]:
                assert 'rule_inputs' in attrs or 'inputs' in attrs
                assert 'rule_outputs' in attrs or 'outputs' in attrs
                attrs['tasks'] = TaskQuerySet(task_ctrl=remake.task_ctrl)
                if remake.config:
                    attrs['config'] = remake.config
                attrs['task_ctrl'] = remake.task_ctrl
                attrs['next_rules'] = set()
                attrs['prev_rules'] = set()
                var_matrix = {}
                for attr, v in attrs.items():
                    if isinstance(v, LoopVar):
                        var_matrix[attr] = v.loop
            elif 'Config' in [b.__name__ for b in bases]:
                pass
        else:
            pass

        newcls = super(RemakeMetaclass, mcs).__new__(
            mcs, clsname, bases, attrs)

        if 'Config' in [b.__name__ for b in bases]:
            remake.config = newcls

        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                remake.rules.append(newcls)
                if not var_matrix:
                    var_matrix = attrs.get('var_matrix', None)
                depends_on = attrs.get('depends_on', tuple())
                if var_matrix:
                    for loop_vars in itertools.product(*var_matrix.values()):
                        fmt_dict = {k: v for k, v in zip(var_matrix.keys(), loop_vars)}
                        # This is a little gnarly.
                        # See: https://stackoverflow.com/questions/41921255/staticmethod-object-is-not-callable
                        # Method has not been bound yet, but you can call it using its __func__ attr.
                        # N.B. both are possible, if e.g. a second rule uses a first rule's method.
                        if hasattr(attrs['rule_inputs'], '__func__'):
                            inputs = attrs['rule_inputs'].__func__(**fmt_dict)
                        elif callable(attrs['rule_inputs']):
                            inputs = attrs['rule_inputs'](**fmt_dict)
                        else:
                            inputs = {k.format(**fmt_dict): fmtp(v, **fmt_dict)
                                      for k, v in attrs['rule_inputs'].items()}
                        if hasattr(attrs['rule_outputs'], '__func__'):
                            outputs = attrs['rule_outputs'].__func__(**fmt_dict)
                        elif callable(attrs['rule_outputs']):
                            outputs = attrs['rule_outputs'](**fmt_dict)
                        else:
                            outputs = {k.format(**fmt_dict): fmtp(v, **fmt_dict)
                                       for k, v in attrs['rule_outputs'].items()}
                        rule_obj = newcls(remake.task_ctrl, attrs['rule_run'], inputs, outputs,
                                          depends_on=depends_on)
                        newcls.tasks.append(rule_obj)
                        remake.task_ctrl.add(rule_obj)
                        for k, v in zip(var_matrix.keys(), loop_vars):
                            setattr(rule_obj, k, v)
                else:
                    rule_obj = newcls(remake.task_ctrl, attrs['rule_run'], attrs['inputs'], attrs['outputs'],
                                      depends_on=depends_on)
                    newcls.tasks.append(rule_obj)
                    remake.task_ctrl.add(rule_obj)

                remake.tasks.extend(newcls.tasks)
        return newcls



class TaskRule(Task, metaclass=RemakeMetaclass):
    pass


# TODO: Ideas for new TaskRules:
# class CommandTaskRule(TaskRule):
#    command = ...
# class ScriptTaskRule(TaskRule):
#    script = ...


class Config(metaclass=RemakeMetaclass):
    pass
