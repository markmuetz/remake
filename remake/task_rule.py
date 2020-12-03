import itertools

from remake import TaskControl, Task


# Can still create a TaskRule() i.e. it's not abstract.
# class RemakeMetaclass(ABCMeta):
class RemakeMetaclass(type):
    all_tasks = []
    config = None
    task_ctrl = TaskControl(__file__)

    def __new__(mcs, clsname, bases, attrs):
        # print(f'creating {clsname}')
        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                assert 'rule_inputs' in attrs or 'inputs' in attrs
                assert 'rule_outputs' in attrs or 'outputs' in attrs
                attrs['tasks'] = []
                if RemakeMetaclass.config:
                    attrs['config'] = RemakeMetaclass.config
            elif 'Config' in [b.__name__ for b in bases]:
                pass
        elif clsname == 'TaskRule':
            attrs['all_tasks'] = RemakeMetaclass.all_tasks
            attrs['task_ctrl'] = RemakeMetaclass.task_ctrl
        else:
            pass

        newcls = super(RemakeMetaclass, mcs).__new__(
            mcs, clsname, bases, attrs)

        if 'Config' in [b.__name__ for b in bases]:
            RemakeMetaclass.config = newcls

        if clsname not in ['TaskRule', 'Config']:
            if 'TaskRule' in [b.__name__ for b in bases]:
                loop_over = attrs.get('var_matrix', None)
                if loop_over:
                    for loop_vars in itertools.product(*loop_over.values()):
                        fmt_dict = {k: v for k, v in zip(loop_over.keys(), loop_vars)}
                        inputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                  for k, v in attrs['rule_inputs'].items()}
                        outputs = {k.format(**fmt_dict): v.format(**fmt_dict)
                                   for k, v in attrs['rule_outputs'].items()}
                        rule_obj = newcls(attrs['rule_run'], inputs, outputs,
                                          is_task_rule=True)
                        newcls.tasks.append(rule_obj)
                        RemakeMetaclass.task_ctrl.add(rule_obj)
                else:
                    rule_obj = newcls(attrs['rule_run'], attrs['inputs'], attrs['outputs'],
                                      is_task_rule=True)
                    newcls.tasks.append(rule_obj)
                    RemakeMetaclass.task_ctrl.add(rule_obj)

                RemakeMetaclass.all_tasks.extend(newcls.tasks)
        return newcls


# metaclass conflict
# class TaskRule(ABC, metaclass=RemakeMetaclass):
class TaskRule(Task, metaclass=RemakeMetaclass):
    pass


class Config(metaclass=RemakeMetaclass):
    pass
