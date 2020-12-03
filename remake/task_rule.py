import itertools

from remake.task import Task
from remake.remake_base import Remake
from remake.task_query_set import TaskQuerySet


class RemakeMetaclass(type):
    config = None

    def __new__(mcs, clsname, bases, attrs):
        # print(f'creating {clsname}')
        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                assert 'rule_inputs' in attrs or 'inputs' in attrs
                assert 'rule_outputs' in attrs or 'outputs' in attrs
                attrs['tasks'] = TaskQuerySet(task_ctrl=Remake.task_ctrl)
                if RemakeMetaclass.config:
                    attrs['config'] = RemakeMetaclass.config
                attrs['task_ctrl'] = Remake.task_ctrl
            elif 'Config' in [b.__name__ for b in bases]:
                pass
        else:
            pass

        newcls = super(RemakeMetaclass, mcs).__new__(
            mcs, clsname, bases, attrs)

        if 'Config' in [b.__name__ for b in bases]:
            RemakeMetaclass.config = newcls

        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                var_matrix = attrs.get('var_matrix', None)
                if var_matrix:
                    for loop_vars in itertools.product(*var_matrix.values()):
                        fmt_dict = {k: v for k, v in zip(var_matrix.keys(), loop_vars)}
                        inputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                  for k, v in attrs['rule_inputs'].items()}
                        outputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                   for k, v in attrs['rule_outputs'].items()}
                        rule_obj = newcls(attrs['rule_run'], inputs, outputs,
                                          is_task_rule=True)
                        newcls.tasks.append(rule_obj)
                        Remake.task_ctrl.add(rule_obj)
                        for k, v in zip(var_matrix.keys(), loop_vars):
                            setattr(rule_obj, k, v)
                else:
                    rule_obj = newcls(attrs['rule_run'], attrs['inputs'], attrs['outputs'],
                                      is_task_rule=True)
                    newcls.tasks.append(rule_obj)
                    Remake.task_ctrl.add(rule_obj)

                Remake.all_tasks.extend(newcls.tasks)
        return newcls


class TaskRule(Task, metaclass=RemakeMetaclass):
    pass


class Config(metaclass=RemakeMetaclass):
    pass
