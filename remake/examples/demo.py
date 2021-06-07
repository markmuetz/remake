"""Simple remake file: in.txt -> fan_out1.txt -> out.txt
                              `> fan_out2.txt /
"""
from remake import Remake, TaskRule

# A remake file is defined by creating a Remake object.
demo = Remake()


class FanOut(TaskRule):
    """Takes one input file and uses two tasks to generate two output files"""
    rule_inputs = {'in': 'data/in.txt'}
    rule_outputs = {'fan_out_{i}': 'data/fan_out_{i}.txt'}
    # This defines the output files, and the number of tasks this TaskRule will create.
    var_matrix = {'i': [1, 2]}

    def rule_run(self):
        # self.inputs and self.outputs are dictionaries created from rule_inputs
        # Each value is a pathlib.Path.
        input_value = self.inputs['in'].read_text()
        self.outputs[f'fan_out_{self.i}'].write_text(f'FanOut {self.i}: {input_value}')


class Out(TaskRule):
    """Takes the two output files produced by FanOut and combines them into one output file"""
    rule_inputs = {f'fan_out_{i}': f'data/fan_out_{i}.txt'
                   for i in [1, 2]}
    rule_outputs = {'out': 'data/out.txt'}

    def rule_run(self):
        input_values = []
        for i in [1, 2]:
            input_values.append(self.inputs[f'fan_out_{i}'].read_text())
        self.outputs['out'].write_text(''.join(input_values))


if __name__ == '__main__':
    # N.B. this file is runnable on its own.
    demo.finalize()
    # Tasks are accessible using the classname of the TaskRule:
    FanOut.tasks.status()
    Out.tasks.status()
    # Or by using the demo object:
    demo.tasks.status()
    # All (remaining) tasks can be run:
    demo.run_all()
