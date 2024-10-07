"""Basic definition which takes in1.txt -> out1.txt -> out2.txt -> out3.txt
"""
from remake import Remake, TaskRule

ex4 = Remake()


def join_lines(path, prepend_text):
    return '\n'.join([f'{prepend_text} {line}' for line in path.read_text().split('\n')[:-1]]) + '\n'


class DependsOn1(TaskRule):
    rule_inputs = {'in': 'data/inputs/in1.txt'}
    rule_outputs = {'out1': 'data/outputs/ex4/out1.txt'}
    depends_on = [join_lines]

    def rule_run(self):
        self.outputs['out1'].write_text(join_lines(self.inputs['in'], 'DependsOn1'))


class DependsOn2(TaskRule):
    rule_inputs = DependsOn1.rule_outputs
    rule_outputs = {'out2': 'data/outputs/ex4/out2.txt'}
    depends_on = [join_lines]

    def rule_run(self):
        self.outputs['out2'].write_text(join_lines(self.inputs['out1'], 'DependsOn2'))


class DependsOn3(TaskRule):
    rule_inputs = DependsOn1.rule_outputs
    rule_outputs = {'out3': 'data/outputs/ex4/out3.txt'}
    depends_on = [join_lines]

    def load_data(self):
        return join_lines(self.inputs['out1'], 'DependsOn3')

    def rule_run(self):
        data = self.load_data()
        self.outputs['out3'].write_text(data)
