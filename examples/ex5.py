"""Basic definition which takes in1.txt -> out1.txt -> out2.txt
"""
from remake import Remake, TaskRule

ex5 = Remake()


class Basic1(TaskRule):
    inputs = {'in': 'data/inputs/in1.txt'}
    outputs = {'out': 'data/outputs/ex5/out1.txt'}

    def rule_run(self):
        assert len(self.inputs) == len(self.outputs)
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text('\n'.join([f'changed output {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


class Basic2(TaskRule):
    inputs = Basic1.outputs
    outputs = {'dummy': 'data/outputs/ex5/dummy.out'}

    def rule_run(self):
        self.outputs['dummy'].touch()


if __name__ == '__main__':
    ex5.finalize()
