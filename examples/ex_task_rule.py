"""Examples using TaskRule"""
from remake import remake_task_control, TaskRule, Config


class MyConfig(Config):
    var1 = 'apples'


class MyRuleMatrix(TaskRule):
    rule_inputs = {'a{i}{j}': 'data/inputs/a.{i}{j}.in'}
    rule_outputs = {'a{i}{j}': 'data/outputs/a.{i}{j}.out'}
    var_matrix = {'i': range(1),
                  'j': range(2)}

    def rule_run(self):
        for o in self.outputs.values():
            o.write_text('output1')


class MyRuleMatrix2(TaskRule):
    rule_inputs = MyRuleMatrix.rule_outputs
    rule_outputs = {'a{i}{j}': 'data/outputs/a.{i}{j}.out2'}
    var_matrix = {'i': range(1),
                  'j': range(2)}

    def rule_run(self):
        for o in self.outputs.values():
            o.touch()


class MyRule(TaskRule):
    inputs = {'a': 'data/inputs/a.in', 'b': 'data/inputs/c.in'}
    outputs = {'a': 'data/outputs/a.out', 'b': 'data/outputs/b.out'}

    def rule_run(self):
        for o in self.outputs.values():
            o.touch()

@remake_task_control
def gen_task_ctrl():
    return TaskRule.task_ctrl
