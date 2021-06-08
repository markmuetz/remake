"""Test minor remake utilities together."""
import logging
import unittest
from unittest import mock
from pathlib import Path

from remake.bcolors import bcolors
from remake.remake_exceptions import RemakeLoadError
from remake.load_remake import load_remake
from remake.remake_base import Remake
from remake.setup_logging import setup_stdout_logging
from remake.special_paths import SpecialPaths, map_special_paths
import remake.util as util
from remake.version import get_version


class TestBcolors(unittest.TestCase):
    def test_print(self):
        bcolors.print('Hi there in bold red', ['BOLD', 'FAIL'])


class TestLoadRemake(unittest.TestCase):
    @mock.patch('remake.load_remake.load_module')
    def test_no_remakes(self, mock_load_module):
        mock_load_module.return_value = []
        self.assertRaisesRegex(RemakeLoadError, 'No remake defined in remake.py', load_remake, 'remake.py')

    @mock.patch('remake.load_remake.load_module')
    def test_too_many_remakes(self, mock_load_module):
        mock_module = mock.MagicMock()
        mock_module.remake_a = Remake('a')
        mock_module.remake_b = Remake('b')
        mock_load_module.return_value = mock_module
        self.assertRaisesRegex(RemakeLoadError, 'More than one remake defined in remake.py', load_remake, 'remake.py')


class TestSetupLogging(unittest.TestCase):
    def setUp(self) -> None:
        remake_root = logging.getLogger('remake')
        if hasattr(remake_root, 'is_setup_stream_logging'):
            del remake_root.is_setup_stream_logging

    def test_setup_stdout_logging_error(self):
        self.assertRaises(ValueError, setup_stdout_logging, 'INFO', True, True)

    def test_setup_stdout_logging_debug(self):
        setup_stdout_logging('DEBUG')

    def test_setup_stdout_logging_detailed(self):
        setup_stdout_logging('INFO', False, True)

    def test_setup_stdout_logging_none(self):
        setup_stdout_logging('INFO', False, False)


class TestSpecialPaths(unittest.TestCase):
    def test_repr(self):
        special_paths = SpecialPaths(P1='/path/1', P2='/path/2')
        repr(special_paths)

    def test_map(self):
        special_paths = SpecialPaths(P1='/path/1', P2='/path/2')
        mapped_paths = map_special_paths(special_paths, {'path1': Path('/path/1/subdir/path.txt')})
        self.assertEqual(mapped_paths['path1'], Path('P1/subdir/path.txt'))


class TestUtil(unittest.TestCase):
    def test_tmp_to_actual(self):
        self.assertRaises(ValueError, util.tmp_to_actual_path, Path('output.txt'))
        self.assertEqual(Path('output.txt'), util.tmp_to_actual_path(Path('.remake.tmp.output.txt')))

    @mock.patch('remake.util.importlib')
    def test_load_module_syntax_error(self, mock_importlib):
        mock_importlib.util.spec_from_file_location.side_effect = SyntaxError()
        # N.B. path must exist in CWD: __file__ does.
        self.assertRaises(SyntaxError, util.load_module, __file__)


class TestVersion(unittest.TestCase):
    def test_all(self):
        print(get_version())
        print(get_version('medium'))
        print(get_version('long'))
        self.assertRaises(ValueError, get_version, 'extra_short')
