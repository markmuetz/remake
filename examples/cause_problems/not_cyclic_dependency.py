from remake import Remake, TaskRule

not_cyclic_dependency = Remake()


class T1(TaskRule):
    inputs = {'in1': 'data/inputs/in1.txt'}
    outputs = {'out1': 'out1.txt'}

    def rule_run(self):
        self.outputs['out1'].touch()


class T2(TaskRule):
    inputs = T1.outputs
    outputs = {'out2': 'out2.txt'}

    def rule_run(self):
        self.outputs['out2'].touch()


class T3(TaskRule):
    inputs = {}
    outputs = {'out3': 'data/inputs/out3.txt'}

    def rule_run(self):
        self.outputs['out3'].touch()


class T4(TaskRule):
    inputs = {**T2.outputs, **T3.outputs}
    outputs = {'out3': 'data/inputs/out4.txt'}

    def rule_run(self):
        self.outputs['out3'].touch()
