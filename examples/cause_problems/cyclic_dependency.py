from remake import Remake, TaskRule

Remake.init()


class T1(TaskRule):
    inputs = {'in1': 'data/inputs/in1.txt'}
    outputs = {'out1': 'out1.txt'}

    def rule_run(self):
        self.outputs['out1'].touch()


class T2(TaskRule):
    inputs = {**T1.outputs, **{'in2': 'data/inputs/in2.txt'}}
    outputs = {'out2': 'out2.txt'}

    def rule_run(self):
        self.outputs['out2'].touch()


class T3(TaskRule):
    inputs = T2.inputs
    outputs = {'out3': 'data/inputs/in2.txt'}

    def rule_run(self):
        self.outputs['out3'].touch()
