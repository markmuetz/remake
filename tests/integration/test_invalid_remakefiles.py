import os
from pathlib import Path
from remake.load_remake import load_remake
from remake.remake_cmd import remake_cmd
from remake.remake_exceptions import RemakeError
import remake

examples_dir = Path(remake.__file__).parent.parent / 'examples'


def test_all_examples():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    example_runner = load_remake('remakefile.py').finalize()
    return
    for task in example_runner.task_ctrl.sorted_tasks:
        yield run_task, example_runner, task
    os.chdir(orig_cwd)


def run_task(example_runner, task):
    example_runner.run_requested([task], True)


def run_task_expect_exception(cmd):
    try:
        remake_cmd(cmd.split())
        raise Exception(f'No exception raised by running: {cmd}')
    except RemakeError:
        pass


def test_invalid_remakefiles():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir / 'invalid_remakefiles')
    for cmd in ['remake run cyclic_dependencies']:
        run_task_expect_exception, cmd
    os.chdir(orig_cwd)
