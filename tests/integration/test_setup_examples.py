import os
import shutil
import tempfile
import unittest
from pathlib import Path

from remake import remake_cmd


class TestSetupExamples(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.origdir = Path.cwd()
        cls.tempdir = Path(tempfile.mkdtemp()) / 'test_setup_examples'
        cls.tempdir.mkdir(exist_ok=True, parents=True)
        os.chdir(cls.tempdir)

    @classmethod
    def tearDownClass(cls) -> None:
        os.chdir(cls.origdir)
        shutil.rmtree(cls.tempdir)

    def test_setup1(self):
        remake_cmd.remake_cmd('remake setup-examples --force'.split())

    def test_setup2(self):
        origdir = Path.cwd()
        os.chdir('remake-examples')
        remake_cmd.remake_cmd('remake run ex1'.split())
        os.chdir(origdir)
