from remake import Remake, Rule, rule_dec

# https://stackoverflow.com/questions/67631/how-can-i-import-a-module-dynamically-given-the-full-path/50395128#50395128
# https://haggis.readthedocs.io/en/latest/api.html#haggis.load.load_module
import sys
import itertools
import pathlib

from loguru import logger

# from tqdm import tqdm

logger.remove()
logger.add(sys.stdout, level='INFO')
# logger.add(sys.stdout, level='DEBUG')


try:
    metadata_manager
    logger.info('Reusing metadata_manager')
except NameError:
    logger.info('Creating metadata_manager')
    create_db = not pathlib.Path(dbloc).exists()
    metadata_manager = Sqlite3MetadataManager()
    if create_db:
        logger.info('creating db')
        metadata_manager.create_db()
        logger.info('created db')
    VirtFS = {}


rmk = Remake()

paths = ['a', 'b']


def f1():
    print('hi')


class Rule1(Rule):
    rule_matrix = {
        'a': [f'a{i}' for i in range(100)],
        'b': [f'b{i}' for i in range(100)],
    }

    # @rule_dec(['paths'])
    @rule_dec
    def rule_inputs(a, b):
        return {f'input_{i}': f'data/{a}/{b}_{i}.in' for i in range(100)}

    @rule_dec
    def rule_outputs(a, b):
        return {'output': f'data/{a}/{b}.out'}

    @rule_dec(['f1', 'VirtFS'])
    def rule_run(inputs, outputs, a, b):
        ''''''
        # f1()
        print(f'Rule1 {a}, {b}')
        for o in outputs.values():
            # VirtFS[o] = f'run {a}, {b}'
            pathlib.Path(o).write_text(f'run {a}, {b}')


class Rule2(Rule):
    rule_matrix = {
        'a': [f'a{i}' for i in range(100)],
        'b': [f'b{i}' for i in range(100)],
    }

    # @rule_dec(['paths'])
    @rule_dec
    def rule_inputs(a, b):
        return {'input': f'data/{a}/{b}.out'}

    @rule_dec
    def rule_outputs(a, b):
        return {'output': f'data/{a}/{b}.out2'}

    # TODO: Would be nice to do this:
    # @rule_dec([f1, 'VirtFS'])
    @rule_dec([f1, VirtFS])
    def rule_run(inputs, outputs, a, b):
        # ''
        print(f'Rule2 {a}, {b}')
        # TODO: How would I get a logger in here?
        # print(f'rule_run({inputs}, {outputs}, {a}, {b})')
        # Extra comment.
        # print('hi')
        for i in inputs.values():
            # print('Prev output:', VirtFS[i])
            print('Prev output:', pathlib.Path(i).read_text())

        # f1()

        for o in outputs.values():
            VirtFS[o] = 'run'
            pathlib.Path(o).write_text('run')


if __name__ == '__main__':
    rmk.autoload_rules()
    # rmk.run()
    # print(VirtFS)
