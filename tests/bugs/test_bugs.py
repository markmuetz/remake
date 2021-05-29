import os
from pathlib import Path
import remake
from remake.remake_cmd import remake_cmd
from remake.util import sysrun

examples_dir = Path.cwd().parent / 'examples'


def test_bug1():
    orig_cwd = os.getcwd()
    os.chdir(examples_dir)
    sysrun('make clean')
    remake_cmd('remake run -E multiproc -f ex1'.split())
    remake_cmd('remake run -E multiproc -f ex1'.split())
    os.chdir(orig_cwd)

