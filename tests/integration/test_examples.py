import os
import io
import shutil
from pathlib import Path
import unittest
from contextlib import redirect_stdout


from remake.remake_cmd import remake_cmd
from remake.util import sysrun

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


class Layer:
    @classmethod
    def setUp(cls):
        cls.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        shutil.rmtree('.remake')

    @classmethod
    def tearDown(cls):
        orig_cwd = os.getcwd()
        # Restore everything to its original state.
        os.chdir(cls.orig_cwd)
        # shutil.rmtree(test_examples_dir)

class TestExamples(unittest.TestCase):
    layer = Layer

    def test_cli(self):
        cmds = [
            'remake info ex1.py',
            'remake info --rule ex1.py',
        ]
        for cmd in cmds:
            yield run_cmd, cmd

    def test_run_ex1(self):
        cmds = [
            'remake run ex1.py',
            'remake run -f ex1.py',
            'rm -rf .remake',
            'remake run -EMultiproc ex1.py',
        ]
        for cmd in cmds:
            yield run_cmd, cmd


def run_cmd(cmd):
    if cmd.split()[0] == 'remake':
        # https://stackoverflow.com/a/40984270/54557
        # Gobble up stdout to stop TMI.
        f = io.StringIO()
        with redirect_stdout(f):
            remake_cmd(cmd.split())
    else:
        sysrun(cmd)
