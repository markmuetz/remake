import os
from pathlib import Path
import unittest
from unittest import mock

from remake.load_remake import load_remake
from remake.monitor import RemakeMonitor, remake_curses_monitor
from remake.util import sysrun

examples_dir = Path(__file__).parent.parent.parent / 'remake' / 'examples'


class TestRemakeMonitor(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')
        self.remake = load_remake('demo.py')

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

    def tearDown(self) -> None:
        os.chdir(self.orig_cwd)


class TestRemakeMonitorCurses(unittest.TestCase):
    def setUp(self) -> None:
        self.orig_cwd = os.getcwd()
        os.chdir(examples_dir)
        sysrun('make clean')
        self.remake = load_remake('ex3.py')
        self.remake.finalize()
        self.monitor = RemakeMonitor(self.remake)

        # Using the real curses package causes problems for PyCharm and github CI.
        # Mock out all the important parts.
        self.stdscr = mock.MagicMock()
        self.stdscr.getmaxyx.return_value = (100, 50)
        # Generate dummy keypresses to feed into RemakeMonitorCurses.
        commands = [
            'r',
            'f',
            't',
            ':task 0',
            ':task 1',
            ':task 2',
            'j',
            'k',
            'g',
            'G',
            'F',
            ':q'  # Note, end by quiting application.
        ]

        clist = []
        for command in commands:
            clist_command = [-1] * 100 + [ord(c) for c in command]
            if len(command) > 1:
                clist_command += [13]
            clist.extend(clist_command)
        self.stdscr.getch.side_effect = clist

        # Create patches for all curses functions called.
        curses_patch_fns = [
            'init_pair',
            'curs_set',
            'color_pair',
            'napms',
            'is_term_resized',
            'resizeterm',
        ]
        self.patchers = []
        for fn in curses_patch_fns:
            patcher = mock.patch(f'curses.{fn}')
            setattr(self, fn, patcher.start())
            self.patchers.append(patcher)
        self.is_term_resized.return_value = False

    def tearDown(self) -> None:
        for patcher in self.patchers:
            patcher.stop()
        os.chdir(self.orig_cwd)

    def test_monitor(self):
        remake_curses_monitor(self.stdscr, self.remake, 1)
