"""Basic definition which takes in1.txt -> out1.txt -> out2.txt
"""
from remake import Remake, TaskRule

Remake.init()


def join_lines(path, prepend_text):
    return '\n'.join([f'{prepend_text} {l}' for l in path.read_text().split('\n')[:-1]]) + '\n'


class DependsOn1(TaskRule):
    inputs = {'in': 'data/inputs/in1.txt'}
    outputs = {'out': 'data/outputs/ex_task_rule4/out1.txt'}
    depends_on = [join_lines]

    def rule_run(self):
        self.outputs['out'].write_text(join_lines(self.inputs['in'], 'DependsOn1'))


class DependsOn2(TaskRule):
    inputs = DependsOn1.outputs
    outputs = {'out': 'data/outputs/ex_task_rule4/out2.txt'}
    depends_on = [join_lines]

    def rule_run(self):

        self.outputs['out'].write_text(join_lines(self.inputs['out'], 'DependsOn2'))
