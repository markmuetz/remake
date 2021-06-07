"""Examples using TaskRule"""
import random
from time import sleep

from remake import Remake, TaskRule
from remake.formatter import remake_dict_expand as dict_exp


slurm_config = {'queue': 'test', 'mem': 64000}
ex2 = Remake(config=dict(slurm=slurm_config))

VAR_MATRIX = {'i': range(4),
              'j': range(4)}


class Init(TaskRule):
    rule_inputs = {}
    rule_outputs = {'out': 'data/outputs/ex2/out1.out'}

    def rule_run(self):
        self.outputs['out'].touch()


class FanOut(TaskRule):
    rule_inputs = Init.rule_outputs
    rule_outputs = {'a{i},{j}': 'data/outputs/ex2/fan_out.{i}.{j}.out'}
    var_matrix = VAR_MATRIX

    def rule_run(self):
        sleep(random.randint(0, 2))
        payload = f'{self.__class__.__name__} ({self.i}, {self.j})'
        for o in self.outputs.values():
            o.write_text(payload)


class Process(TaskRule):
    rule_inputs = FanOut.rule_outputs
    rule_outputs = {'a{i},{j}': 'data/outputs/ex2/process.{i}.{j}.out'}
    var_matrix = VAR_MATRIX

    def rule_run(self):
        sleep(random.randint(0, 2))
        # Arbitrary CPU-bound calculation that slows down this task.
        total = sum(range(int(1e2) + self.i + self.j))
        payload = f'{self.__class__.__name__} ({self.i}, {self.j}) {total}'
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text(i.read_text() + payload)


class Reduce1(TaskRule):
    rule_inputs = dict_exp(Process.rule_outputs, j=VAR_MATRIX['j'])
    rule_outputs = {'a{i}': 'data/outputs/ex2/reduce1.{i}.out'}
    var_matrix = {'i': VAR_MATRIX['i']}

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.i})'
        payload += ', '.join([i.read_text() for i in self.inputs.values()])
        for o in self.outputs.values():
            o.write_text(payload)


class Reduce2(TaskRule):
    rule_inputs = dict_exp(Reduce1.rule_outputs, i=VAR_MATRIX['i'])
    rule_outputs = {'a': 'data/outputs/ex2/reduce2.out'}

    def rule_run(self):
        payload = f'{self.__class__.__name__}'
        payload += ', '.join([i.read_text() for i in self.inputs.values()])
        for o in self.outputs.values():
            o.write_text(payload)


if __name__ == '__main__':
    ex2.finalize()
