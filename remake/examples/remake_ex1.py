from pathlib import Path

from remake import Remake, TaskRule

rmk = Remake(config=dict(content_checks=False))

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
        return {f'input_{i}': Path.cwd() / f'data/{a}/{b}_{i}.in' for i in range(20)}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': Path.cwd() / f'data/{a}/{b}.orig_out'}

    def rule_run(self):
        ''''''
        # print('hi')
        # ''
        # f1()
        # print(f'Rule1 {self.a}, {self.b}')
        for o in self.outputs.values():
            o.write_text(f'run {self.a}, {self.b}')


class Rule2(TaskRule):
    var_matrix = {
        'a': [f'a{i}' for i in range(100)],
        'b': [f'b{i}' for i in range(20)],
    }

    @staticmethod
    def rule_inputs(a, b):
        return {'input': Path.cwd() / f'data/{a}/{b}.orig_out'}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': Path.cwd() / f'data/{a}/{b}.orig_out2'}

    def rule_run(self):
        # ''
        # print(f'Rule2 {self.a}, {self.b}')
        # TODO: How would I get a logger in here?
        # print(f'rule_run({inputs}, {outputs}, {a}, {b})')
        # Extra comment.
        # print('hi')
        for i in self.inputs.values():
            # print('Prev output:', i.read_text())
            pass

        for o in self.outputs.values():
            o.write_text('run')
