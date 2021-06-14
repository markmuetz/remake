import os
from pathlib import Path
from remake.loader import load_remake
from remake.util import sysrun

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'

orig_cwd = None


def setup_module():
    global orig_cwd
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)


def teardown_module():
    global orig_cwd
    orig_cwd = os.getcwd()
    # Restore everything to its original state.
    sysrun('make reset')
    os.chdir(orig_cwd)


def test_all_examples():
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


def run_task(example_runner, task):
    example_runner.run_requested([task], True)
