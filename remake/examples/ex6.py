from remake import Remake, TaskRule

rmk = Remake()


class Init(TaskRule):
    rule_inputs = {}
    rule_outputs = {'out0': 'data/outputs/ex6/rule1.0.in',
                    'out1': 'data/outputs/ex6/chain.0.in'}

    def rule_run(self):
        self.outputs['out0'].touch()
        self.outputs['out1'].touch()


class Rule1(TaskRule):
    """Links to Rule2 and back"""
    rule_inputs = {'in': 'data/outputs/ex6/rule1.{i}.in'}
    rule_outputs = {'out': 'data/outputs/ex6/rule1.{i}.out'}
    var_matrix = {'i': range(4)}

    def rule_run(self):
        self.outputs['out'].touch()


class Rule2(TaskRule):
    """Links to Rule1 and back"""
    rule_inputs = {'in': 'data/outputs/ex6/rule1.{i}.out'}

    @staticmethod
    def rule_outputs(i):
        return {'out': f'data/outputs/ex6/rule1.{i + 1}.in'}
    var_matrix = {'i': range(4)}

    def rule_run(self):
        self.outputs['out'].touch()


class Chain(TaskRule):
    """Links to itself"""
    rule_inputs = {'in': 'data/outputs/ex6/chain.{i}.in'}

    @staticmethod
    def rule_outputs(i):
        return {'out': f'data/outputs/ex6/chain.{i + 1}.in'}
    var_matrix = {'i': range(4)}

    def rule_run(self):
        self.outputs['out'].touch()


if __name__ == '__main__':
    rmk.finalize()
