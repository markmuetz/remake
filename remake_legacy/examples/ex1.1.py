"""Basic definition which takes in1.txt -> out1.txt -> out2.txt
"""
from remake import Remake, TaskRule

ex1 = Remake()


class Basic1(TaskRule):
    rule_inputs = {'in': 'data/inputs/in1.txt'}
    rule_outputs = {'out': 'data/outputs/ex1/out1.txt'}

    def rule_run(self):
        # Changed!
        print('changed')
        assert len(self.inputs) == len(self.outputs)
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text('\n'.join([f'Basic1 {line}' for line in i.read_text().split('\n')[:-1]]) + '\n')


class Basic2(TaskRule):
    rule_inputs = Basic1.rule_outputs
    rule_outputs = {'out': 'data/outputs/ex1/out2.txt'}

    def rule_run(self):

        assert len(self.inputs) == len(self.outputs)
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text('\n'.join([f'f1 {line}' for line in i.read_text().split('\n')[:-1]]) + '\n')
