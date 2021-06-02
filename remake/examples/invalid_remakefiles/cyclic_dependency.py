from remake import Remake, TaskRule

cyclic_dependency = Remake()


class T1(TaskRule):
    rule_inputs = {'in1': 'data/inputs/in1.txt'}
    rule_outputs = {'out1': 'data/out1.txt'}

    def rule_run(self):
        self.rule_outputs['out1'].touch()


class T2(TaskRule):
    rule_inputs = {**T1.rule_outputs, **{'in2': 'data/inputs/in2.txt'}}
    rule_outputs = {'out2': 'data/out2.txt'}

    def rule_run(self):
        self.outputs['out2'].touch()


class T3(TaskRule):
    rule_inputs = T2.rule_inputs
    rule_outputs = {'out3': 'data/inputs/in2.txt'}

    def rule_run(self):
        self.outputs['out3'].touch()
