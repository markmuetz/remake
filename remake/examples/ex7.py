"""Basic definition which takes in1.txt -> out1.txt -> out2.txt
"""
from remake import Remake, TaskRule

ex7 = Remake()


class Basic1(TaskRule):
    rule_inputs = {'in1': 'data/inputs/in1.txt',
                   'in2': 'data/inputs/in2.txt'}
    rule_outputs = {'out': 'data/outputs/ex7/out1.txt'}

    def rule_run(self):
        input_text = self.inputs['in1'].read_text() + '\n' + self.inputs['in2'].read_text()
        self.outputs['out'].write_text(input_text + '\n')


class Basic2(TaskRule):
    rule_inputs = Basic1.rule_outputs
    rule_outputs = {'out': 'data/outputs/ex7/out2.txt'}

    def rule_run(self):

        assert len(self.inputs) == len(self.outputs)
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text('\n'.join([f'f1 {line}' for line in i.read_text().split('\n')[:-1]]) + '\n')


if __name__ == '__main__':
    ex7.finalize()
