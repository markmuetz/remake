"""Simple remake file: in.txt -> fan_out1.txt -> out.txt
                              `> fan_out2.txt /
"""
from remake import Remake, TaskRule

demo = Remake()


class FanOut(TaskRule):
    rule_inputs = {'in': 'data/in.txt'}
    rule_outputs = {'fan_out_{i}': 'data/fan_out_{i}.txt'}
    var_matrix = {'i': [1, 2]}

    def rule_run(self):
        # self.inputs and self.outputs are dictionaries created from rule_inputs
        # Each value is a pathlib.Path.
        input_value = self.inputs['in'].read_text()
        self.outputs[f'fan_out_{self.i}'].write_text(f'FanOut {self.i}: {input_value}')


class Out(TaskRule):
    rule_inputs = {f'fan_out_{i}': f'data/fan_out_{i}.txt'
                   for i in [1, 2]}
    rule_outputs = {'out': 'data/out.txt'}

    def rule_run(self):
        input_values = []
        for i in [1, 2]:
            input_values.append(self.inputs[f'fan_out_{i}'].read_text())
        self.outputs['out'].write_text(''.join(input_values))

