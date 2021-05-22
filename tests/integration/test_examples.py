import os
from pathlib import Path
from remake.load_remake import load_remake
import remake

examples_dir = Path(remake.__file__).parent.parent / 'examples'


def test_all_examples():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    example_runner = load_remake('remakefile.py').finalize()
    for task in example_runner.task_ctrl.sorted_tasks:
        yield run_task, example_runner, task
    os.chdir(orig_cwd)


def run_task(example_runner, task):
    example_runner.run_requested([task], True)