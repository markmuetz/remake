import os
from pathlib import Path
import unittest
from unittest import mock

from remake.loader import load_remake
from remake.monitor import RemakeMonitor, remake_curses_monitor
from remake.util import sysrun

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


class TestRemakeMonitor(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')
        cls.remake = load_remake('demo.py')

    @classmethod
    def tearDownClass(cls) -> None:
        sysrun('make reset')
        os.chdir(cls.orig_cwd)

    def test_monitor1(self):
        print(self.remake)
        monitor = RemakeMonitor(self.remake)
        monitor.refresh()
        for task, status in monitor.statuses:
            assert status in ['remaining', 'pending']
        self.remake.finalize()
        self.remake.run_all()
        for task, status in monitor.statuses:
            assert status == 'completed'


class TestRemakeMonitorCurses(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')
        cls.remake = load_remake('ex3.py')
        cls.remake.finalize()
        cls.monitor = RemakeMonitor(cls.remake)

        # Using the real curses package causes problems for PyCharm and github CI.
        # Mock out all the important parts.
        cls.stdscr = mock.MagicMock()
        cls.stdscr.getmaxyx.return_value = (100, 50)
        # Generate dummy keypresses to feed into RemakeMonitorCurses.
        commands = [
            'r',
            'f',
            't',
            ':task 0',
            ':task 1',
            ':task 2',
            ':show tasks',
            'w'
            'j',
            'k',
            'g',
            'G',
            'F',
            'R',
            ':q'  # Note, end by quiting application.
        ]

        clist = []
        for command in commands:
            clist_command = [-1] * 100 + [ord(c) for c in command]
            if len(command) > 1:
                clist_command += [13]
            clist.extend(clist_command)
        cls.stdscr.getch.side_effect = clist

        # Create patches for all curses functions called.
        curses_patch_fns = [
            'init_pair',
            'curs_set',
            'color_pair',
            'napms',
            'is_term_resized',
            'resizeterm',
        ]
        cls.patchers = []
        for fn in curses_patch_fns:
            patcher = mock.patch(f'curses.{fn}')
            setattr(cls, fn, patcher.start())
            cls.patchers.append(patcher)
        cls.is_term_resized.return_value = False

    @classmethod
    def tearDownClass(cls) -> None:
        for patcher in cls.patchers:
            patcher.stop()
        sysrun('make reset')
        os.chdir(cls.orig_cwd)

    def test_monitor(self):
        remake_curses_monitor(self.stdscr, self.remake, 1)
