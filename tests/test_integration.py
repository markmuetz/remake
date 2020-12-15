import os
from remake.load_task_ctrls import load_remake


def test_all_examples():
    orig_cwd = os.getcwd()
    os.chdir('../examples')
    example_runner = load_remake('remakefile.py').finalize()
    for task in example_runner.task_ctrl.sorted_tasks:
        yield run_task, example_runner, task
    os.chdir(orig_cwd)


def run_task(example_runner, task):
    example_runner.run_requested([task], True)
