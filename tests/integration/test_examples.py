import os
from pathlib import Path
from remake.load_remake import load_remake
from remake.util import sysrun

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


def test_all_examples():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    example_runner = load_remake('test_all_examples.py').finalize()
    for task in example_runner.tasks.in_rule('RunAllRemakes').filter(executor='singleproc'):
        yield run_task, example_runner, task
    for task in example_runner.tasks.in_rule('RunAllRemakes').filter(executor='multiproc'):
        yield run_task, example_runner, task
    for task in example_runner.tasks.in_rule('TestCLI'):
        yield run_task, example_runner, task
    for task in example_runner.tasks.in_rule('TestCLI2'):
        yield run_task, example_runner, task
    for task in example_runner.tasks.in_rule('TestEx1'):
        yield run_task, example_runner, task
    # Restore everything to its original state.
    sysrun('make reset')
    os.chdir(orig_cwd)


def run_task(example_runner, task):
    example_runner.run_requested([task], True)
