import os
from pathlib import Path
import unittest
from curses import wrapper

from remake.load_remake import load_remake
from remake.monitor import RemakeMonitor, RemakeMonitorCurses, remake_curses_monitor
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

    def test_monitor1(self):
        class StatefulCaller:
            def __init__(self):
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
                    ':q'
                ]

                clist = []
                for command in commands:
                    clist_command = [-1] * 100 + list(command)
                    if len(command) > 1:
                        clist_command += [13]
                    clist.extend(clist_command)
                self.ch_iter = iter(clist)

            def __call__(self):
                try:
                    c = next(self.ch_iter)
                except StopIteration:
                    return -1
                if isinstance(c, int):
                    return c
                else:
                    return ord(c)

        c = StatefulCaller()
        def getch(_mon):
            return c()

        RemakeMonitorCurses.getch = getch
        wrapper(remake_curses_monitor, self.remake, 1)

    def tearDown(self) -> None:
        os.chdir(self.orig_cwd)


if __name__ == '__main__':
    unittest.main()
