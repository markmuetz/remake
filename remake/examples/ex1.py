from remake import Remake, Rule
from pathlib import Path


rmk = Remake(config=dict(check_outputs_exist=True))


class Rule1(Rule):
    rule_matrix = {
        'a': [f'a{i}' for i in range(6)],
        'b': [f'b{i}' for i in range(5)],
    }
    rule_inputs = {f'input_{i}': f'data/{{a}}/{{b}}_{i}.in' for i in range(4)}
    rule_outputs = {'output': 'data/outputs/{a}/{b}.rule1.out'}

    def rule_run(inputs, outputs, a, b):
        for o in outputs.values():
            Path(o).write_text(f'run {a}, {b}')


class Rule2(Rule):
    rule_matrix = Rule1.rule_matrix
    rule_inputs = Rule1.rule_outputs
    rule_outputs = {'output': 'outputs/data/{a}/{b}.rule2.out'}

    def rule_run(inputs, outputs, a, b):
        for i in inputs.values():
            pass

        for o in outputs.values():
            Path(o).write_text('run')
