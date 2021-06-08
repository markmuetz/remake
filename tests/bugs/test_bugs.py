import os
from pathlib import Path

from remake.load_remake import load_remake
from remake.remake_cmd import remake_cmd
from remake.util import sysrun
from remake.special_paths import is_relative_to

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'
bugs_remakefiles_dir = Path(__file__).parent / 'bugs_remakefiles'


def test_bug1():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    sysrun('make clean')
    remake_cmd('remake run -E multiproc -f ex1'.split())
    remake_cmd('remake run -E multiproc -f ex1'.split())
    sysrun('make reset')
    os.chdir(orig_cwd)


def test_bug2():
    """Absolute paths should end up with metadata under .remake"""
    orig_cwd = os.getcwd()
    os.chdir(bugs_remakefiles_dir)
    remake = load_remake('absolute_paths.py')
    task_md = remake.tasks[0].task_md
    path_md = list(task_md.inputs_metadata_map.values())[0]
    assert is_relative_to(path_md.metadata_path, Path('.remake'))
    os.chdir(orig_cwd)


def test_bug3():
    """remake run --one not working #29"""
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    sysrun('make clean')
    remake_cmd('remake run --one ex1'.split())
    sysrun('make reset')
    os.chdir(orig_cwd)


def test_bug4():
    """remake run --one not working #29"""
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    sysrun('make clean')
    ex1 = load_remake('ex1.py')
    ex1.finalize()
    ex1.run_one()
    sysrun('make reset')
    os.chdir(orig_cwd)
