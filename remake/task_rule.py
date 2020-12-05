import itertools

from remake.task import Task
from remake.remake_base import Remake
from remake.task_query_set import TaskQuerySet


class RemakeMetaclass(type):
    def __new__(mcs, clsname, bases, attrs):
        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                assert 'rule_inputs' in attrs or 'inputs' in attrs
                assert 'rule_outputs' in attrs or 'outputs' in attrs
                attrs['tasks'] = TaskQuerySet(task_ctrl=Remake.task_ctrl)
                if Remake.config:
                    attrs['config'] = Remake.config
                attrs['task_ctrl'] = Remake.task_ctrl
                attrs['next_rules'] = set()
                attrs['prev_rules'] = set()
            elif 'Config' in [b.__name__ for b in bases]:
                pass
        else:
            pass

        newcls = super(RemakeMetaclass, mcs).__new__(
            mcs, clsname, bases, attrs)

        if 'Config' in [b.__name__ for b in bases]:
            Remake.config = newcls

        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                Remake.rules.append(newcls)
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
                            inputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                      for k, v in attrs['rule_inputs'].items()}
                        if hasattr(attrs['rule_outputs'], '__func__'):
                            outputs = attrs['rule_outputs'].__func__(**fmt_dict)
                        elif callable(attrs['rule_outputs']):
                            outputs = attrs['rule_outputs'](**fmt_dict)
                        else:
                            outputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                       for k, v in attrs['rule_outputs'].items()}
                        rule_obj = newcls(Remake.task_ctrl, attrs['rule_run'], inputs, outputs,
                                          depends_on=depends_on)
                        newcls.tasks.append(rule_obj)
                        Remake.task_ctrl.add(rule_obj)
                        for k, v in zip(var_matrix.keys(), loop_vars):
                            setattr(rule_obj, k, v)
                else:
                    rule_obj = newcls(Remake.task_ctrl, attrs['rule_run'], attrs['inputs'], attrs['outputs'],
                                      depends_on=depends_on)
                    newcls.tasks.append(rule_obj)
                    Remake.task_ctrl.add(rule_obj)

                Remake.tasks.extend(newcls.tasks)
        return newcls


class TaskRule(Task, metaclass=RemakeMetaclass):
    pass


class Config(metaclass=RemakeMetaclass):
    pass
