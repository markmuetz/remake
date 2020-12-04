"""Examples using TaskRule"""
from remake import Remake, TaskRule
from remake.formatter import remake_dict_expand as dict_exp


Remake.init()

VAR_MATRIX = {'i': range(10),
              'j': range(10)}


class FanOut(TaskRule):
    rule_inputs = {}
    rule_outputs = {'a{i},{j}': 'data/outputs/ex_task_rule2/fan_out.{i}.{j}.out'}
    var_matrix = VAR_MATRIX

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.i}, {self.j})'
        for o in self.outputs.values():
            o.write_text(payload)


class Process(TaskRule):
    rule_inputs = FanOut.rule_outputs
    rule_outputs = {'a{i},{j}': 'data/outputs/ex_task_rule2/process.{i}.{j}.out'}
    var_matrix = VAR_MATRIX

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.i}, {self.j})'
        for i, o in zip(self.inputs.values(), self.outputs.values()):
            o.write_text(i.read_text() + payload)


class Reduce1(TaskRule):
    rule_inputs = dict_exp(Process.rule_outputs, j=VAR_MATRIX['j'])
    rule_outputs = {'a{i}': 'data/outputs/ex_task_rule2/reduce1.{i}.out'}
    var_matrix = {'i': VAR_MATRIX['i']}

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.i})'
        payload += ', '.join([i.read_text() for i in self.inputs.values()])
        for o in self.outputs.values():
            o.write_text(payload)


class Reduce2(TaskRule):
    inputs = dict_exp(Reduce1.rule_outputs, i=VAR_MATRIX['i'])
    outputs = {'a': 'data/outputs/ex_task_rule2/reduce2.out'}

    def rule_run(self):
        payload = f'{self.__class__.__name__}'
        payload += ', '.join([i.read_text() for i in self.inputs.values()])
        for o in self.outputs.values():
            o.write_text(payload)

