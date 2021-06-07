"""Demonstrates partial run when some input data not there.
"""
from remake import Remake, TaskRule

ex8 = Remake()


class CannotRun(TaskRule):
    rule_inputs = {'in1': 'data/inputs/input_not_there.txt'}
    rule_outputs = {'out': 'data/inputs/ex8_in1.txt'}

    def rule_run(self):
        input_text = self.inputs['in1'].read_text()
        self.outputs['out'].write_text(input_text + '\n')


class CanRun1(TaskRule):
    rule_inputs = CannotRun.rule_outputs
    rule_outputs = {'out1': 'data/outputs/ex8/out1.txt',
                    'out2': 'data/outputs/ex8/out2.txt'}

    def rule_run(self):
        for o in self.outputs.values():
            o.write_text('out')


class CanRun2(TaskRule):
    rule_inputs = {'in': 'data/outputs/ex8/out{i}.txt'}
    rule_outputs = {'out1': 'data/outputs/ex8/out2.{i}.txt'}
    var_matrix = {'i': [1, 2]}

    def rule_run(self):
        assert len(self.inputs) == len(self.outputs)
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text('\n'.join([f'f1 {line}' for line in i.read_text().split('\n')[:-1]]) + '\n')


if __name__ == '__main__':
    ex8.finalize()
