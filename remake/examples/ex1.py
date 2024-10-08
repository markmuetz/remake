import sys
from loguru import logger

logger.remove()
logger.add(sys.stdout, level='TRACE')
logger.trace('importing')

if sys.argv[1] == 'remake_legacy':
    from remake_legacy import Remake, TaskRule
    from pathlib import Path

    P2 = Path
elif sys.argv[1] == 'remake':
    from remake import Remake, TaskRule
    from pathlib import Path

    P2 = Path


logger.trace('imported')


logger.trace('create rmk')
if sys.argv[1] == 'remake_legacy':
    rmk = Remake(config=dict(content_checks=False))
elif sys.argv[1] == 'remake':
    rmk = Remake()
logger.trace('created rmk')


logger.trace('create Rule1')


class Rule1(TaskRule):
    var_matrix = {
        'a': [f'a{i}' for i in range(30)],
        'b': [f'b{i}' for i in range(20)],
    }

    @staticmethod
    def rule_inputs(a, b):
        return {f'input_{i}': P2(f'data/{a}/{b}_{i}.in') for i in range(20)}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': P2(f'data/{a}/{b}.{sys.argv[1]}.rule1.out')}

    def rule_run(self):
        # ''
        for o in self.outputs.values():
            Path(o).write_text(f'run {self.a}, {self.b}')


logger.trace('created Rule1')

logger.trace('create Rule2')


class Rule2(TaskRule):
    var_matrix = {
        'a': [f'a{i}' for i in range(30)],
        'b': [f'b{i}' for i in range(20)],
    }

    @staticmethod
    def rule_inputs(a, b):
        return {'input': P2(f'data/{a}/{b}.{sys.argv[1]}.rule1.out')}

    @staticmethod
    def rule_outputs(a, b):
        return {'output': P2(f'data/{a}/{b}.{sys.argv[1]}.rule2.out')}

    def rule_run(self):
        ''''''
        for i in self.inputs.values():
            pass

        for o in self.outputs.values():
            Path(o).write_text('run')


logger.trace('create Rule2')

if __name__ == '__main__':
    if sys.argv[1] == 'remake_legacy':
        logger.trace('finalize')
        rmk.finalize()
        logger.trace('finalized')

        logger.trace('run')
        rmk.run_all()
        logger.trace('run finished')
    elif sys.argv[1] == 'remake':
        logger.trace('finalize')
        rmk.autoload_rules()
        logger.trace('finalized')

        logger.trace('run')
        rmk.run()
        logger.trace('run finished')
