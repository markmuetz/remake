from remake2 import Remake, TaskRule

# https://stackoverflow.com/questions/67631/how-can-i-import-a-module-dynamically-given-the-full-path/50395128#50395128
# https://haggis.readthedocs.io/en/latest/api.html#haggis.load.load_module
import sys
import pathlib


rmk = Remake()

paths = ['a', 'b']


def f1():
    print('hi')


class Rule1(TaskRule):
    var_matrix = {
        'a': [f'a{i}' for i in range(100)],
        'b': [f'b{i}' for i in range(20)],
    }

    @staticmethod
    def rule_inputs(a, b):
        return {f'input_{i}': f'data/{a}/{b}_{i}.in' for i in range(20)}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': f'data/{a}/{b}.out'}

    def rule_run(self):
        ''''''
        # f1()
        # print(f'Rule1 {self.a}, {self.b}')
        for o in self.outputs.values():
            # VirtFS[o] = f'run {a}, {b}'
            pathlib.Path(o).write_text(f'run {self.a}, {self.b}')


class Rule2(TaskRule):
    var_matrix = {
        'a': [f'a{i}' for i in range(100)],
        'b': [f'b{i}' for i in range(20)],
    }

    @staticmethod
    def rule_inputs(a, b):
        return {'input': f'data/{a}/{b}.out'}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': f'data/{a}/{b}.out2'}

    def rule_run(self):
        ''''''
        # print(f'Rule2 {self.a}, {self.b}')
        for i in self.inputs.values():
            # print('Prev output:', VirtFS[i])
            # print('Prev output:', pathlib.Path(i).read_text())
            pass

        # f1()

        for o in self.outputs.values():
            pathlib.Path(o).write_text('run')


if __name__ == '__main__':
    rmk.autoload_rules()
    rmk.run()
