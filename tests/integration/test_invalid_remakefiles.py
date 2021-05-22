import os
from pathlib import Path
from remake.remake_cmd import remake_cmd
from remake.remake_exceptions import CyclicDependency
import remake

examples_dir = Path(remake.__file__).parent.parent / 'examples'


def run_task_expect_exception(cmd, expected_exception):
    try:
        remake_cmd(cmd.split())
    except Exception as e:
        if e.__class__ != expected_exception:
            raise Exception(f'Wrong exception: expected {expected_exception}, got {e.__class__}')
        return
    raise Exception(f'No exception of type {expected_exception} raised by running: {cmd}')


def test_invalid_remakefiles():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir / 'invalid_remakefiles')
    invalid_remakefiles = sorted(Path.cwd().glob('*.py'))
    for cmd in [f'remake run {p.name}' for p in invalid_remakefiles]:
        yield run_task_expect_exception, cmd, CyclicDependency
    os.chdir(orig_cwd)
