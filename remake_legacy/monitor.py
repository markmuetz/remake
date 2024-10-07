from collections import Counter
import datetime as dt
from pathlib import Path
import logging

import curses

from remake import Remake
from remake.loader import load_remake
from remake.metadata import METADATA_VERSION
from remake.util import sha1sum

logger = logging.getLogger(__name__)


class Quit(Exception):
    pass


class RemakeMonitor:
    def __init__(self, remake):
        self.remake = remake
        self.status_dir = Path(remake.task_ctrl.dotremake_dir / METADATA_VERSION /
                               remake.name / 'task_status')

    def refresh(self):
        # paths = sorted(self.status_dir.glob('*/*.status'))
        self.status_counts = Counter()
        self.task_key_status_map = {}

        self.statuses = []
        for task in self.remake.task_ctrl.sorted_tasks:
            key = task.path_hash_key()
            task_status_path = task.task_md.task_status_path
            if not task_status_path.exists():
                status = 'UNKNOWN'
            else:
                time, status = task_status_path.read_text().split('\n')[-2].split(';')
            self.status_counts[status] += 1
            self.task_key_status_map[key] = status
            self.statuses.append((task, status))


class RemakeMonitorCurses:
    def __init__(self, stdscr, remake: Remake, timeout: float, wrap=False):
        self.stdscr = stdscr
        self.remake = remake
        self.monitor = RemakeMonitor(remake)
        self.remake_sha1sum = sha1sum(Path(remake.name + '.py'))
        self.timeout = int(timeout * 1000)
        self.input_loop_timeout = 10
        self.num_input_loops = self.timeout // self.input_loop_timeout
        self.wrap = wrap

        # Remove stream logging.
        remake_root = logging.getLogger('remake')
        handlers = [h for h in remake_root.handlers
                    if isinstance(h, logging.StreamHandler)]
        for handler in handlers:
            remake_root.debug(f'Removing stream handler {handler}')
            remake_root.handlers.remove(handler)

        self.rows, self.cols = stdscr.getmaxyx()
        self.colour_pairs = {
            "CANNOT_RUN": 1,
            "PENDING": 2,
            "REMAINING": 3,
            "RUNNING": 4,
            "COMPLETED": 5,
            "ERROR": 6,
            "UNKNOWN": 7,
        }
        curses.init_pair(self.colour_pairs['CANNOT_RUN'], curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['PENDING'], curses.COLOR_MAGENTA, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['REMAINING'], curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['RUNNING'], curses.COLOR_BLUE, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['COMPLETED'], curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['ERROR'], curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(self.colour_pairs['UNKNOWN'], curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_WHITE)

        self.stdscr.nodelay(True)
        curses.curs_set(0)

    def cp(self, status):
        return curses.color_pair(self.colour_pairs[status])

    def ui(self, mode, command, keypresses, show, remake_name):
        stdscr = self.stdscr
        stdscr.clear()
        timestr = f'{dt.datetime.now().replace(microsecond=0)}'
        stdscr.addstr(0, self.cols // 2 - len(timestr) // 2, timestr)

        topline = [' '] * self.cols
        show_str = f' {show} '
        topline[self.cols // 2 - len(show_str): len(show_str)] = list(show_str)
        stdscr.addstr(1, 0, ''.join(topline), curses.color_pair(8))

        bottomline = [' '] * self.cols
        bottomline[:len(remake_name)] = list(remake_name)
        stdscr.addstr(self.rows - 2, 0, ''.join(bottomline), curses.color_pair(8) | curses.A_BOLD)

    def summary(self, status_counts):
        stdscr = self.stdscr
        stdscr.addstr(2, 0, f'Cant run : {status_counts["CANNOT_RUN"]}', self.cp('CANNOT_RUN'))
        stdscr.addstr(3, 0, f'Unknown  : {status_counts["UNKNOWN"]}', self.cp('UNKNOWN'))
        stdscr.addstr(4, 0, f'Remaining: {status_counts["REMAINING"]}', self.cp('REMAINING'))
        stdscr.addstr(5, 0, f'Pending  : {status_counts["PENDING"]}', self.cp('PENDING'))
        stdscr.addstr(6, 0, f'Running  : {status_counts["RUNNING"]}', self.cp('RUNNING'))
        stdscr.addstr(7, 0, f'Completed: {status_counts["COMPLETED"]}', self.cp('COMPLETED'))
        stdscr.addstr(8, 0, f'Error    : {status_counts["ERROR"]}', self.cp('ERROR'))

    def _check_i_offset(self, i_offset, n):
        if i_offset < -n + self.rows - 4 or i_offset == -10000:
            i_offset = -n + self.rows - 4
        if i_offset > 0:
            i_offset = 0
        return i_offset

    def show_tasks(self):
        monitor = self.monitor

        output = []
        for i, (task, status) in enumerate(monitor.statuses):
            output.append((f'{str(i):>3} {status:<10}: {task}', self.cp(status)))
        return output

    def show_rules(self):
        remake = self.remake
        monitor = self.monitor
        output = []
        output.append(' ' * 20 + '  CR,  U,  P, RM,  R,  C,  E')
        for i, rule in enumerate(remake.rules):
            line = []
            line.append(f'{str(rule.__name__)[:20]:<20} ')
            rule_status = Counter([monitor.task_key_status_map[t.path_hash_key()]
                                  for t in rule.tasks
                                  if t.path_hash_key() in monitor.task_key_status_map])
            for j, status in enumerate(['CANNOT_RUN', 'UNKNOWN', 'PENDING',
                                        'REMAINING', 'RUNNING',
                                        'COMPLETED', 'ERROR']):

                line.append(f'{str(rule_status[status]):>3}')

            output.append(line[0] + ','.join(line[1:]))
        return output

    def show_files(self):
        remake = self.remake
        paths = [p
                 for t in remake.task_ctrl.sorted_tasks
                 for p in t.outputs.values()]
        output = []
        for i, path in enumerate(paths):
            if path.exists():
                output.append((f'{str(path.exists()):>5}: {path}',
                               self.cp('COMPLETED')))
            else:
                output.append(f'{str(path.exists()):>5}: {path}')
        return output

    def show_task(self, task_i):
        task = list(self.remake.task_ctrl.sorted_tasks.keys())[task_i]

        output = [f'{task.path_hash_key()[:6]}: {task}']

        task.task_md.generate_metadata()
        task.task_md.task_requires_rerun()

        if task.task_md.rerun_reasons:
            output.append('===RERUN REASONS===')
            for reason in task.task_md.rerun_reasons:
                if reason[1]:
                    output.append(f'  {reason[0]}: {reason[1]}')
                else:
                    output.append(f'  {reason[0]}')

        task_diff = task.diff()
        if task_diff:
            output.append('===DIFF===')
            output.extend(task_diff)

        if task.task_md.log_path.exists():
            output.append('===LOG===')
            for line in task.task_md.log_path.read_text().split('\n'):
                if not line:
                    continue
                output.append(f'{line}')
        return output

    def display_output(self, output, i_offset):
        wrapped_output = []
        if self.wrap:
            for line in output:
                if isinstance(line, tuple) and len(line) == 2:
                    line, colour = line
                else:
                    colour = None
                while 15 + len(line) > self.cols:
                    wrapped_output.append((line[:self.cols - 15], colour))
                    line = line[self.cols - 15:]
                wrapped_output.append((line, colour))
        else:
            for line in output:
                if isinstance(line, tuple) and len(line) == 2:
                    line, colour = line
                else:
                    colour = None
                wrapped_output.append((line[:self.cols - 15], colour))

        i_offset = self._check_i_offset(i_offset, len(wrapped_output))
        for i, (line, colour) in enumerate(wrapped_output):
            if 2 + i + i_offset <= 1:
                continue
            if 2 + i + i_offset >= self.rows - 2:
                break
            if colour:
                self.stdscr.addstr(i + 2 + i_offset, 15, line, colour)
            else:
                self.stdscr.addstr(i + 2 + i_offset, 15, line)

    def getch(self):
        return self.stdscr.getch()

    def input_loop(self, mode, command, keypresses, show, i_offset):
        stdscr = self.stdscr
        for i in range(self.num_input_loops):
            # Input loop.
            curses.napms(self.input_loop_timeout)
            # Action in loop if resize is True:
            # Not working!
            if curses.is_term_resized(self.rows, self.cols):
                self.rows, self.cols = self.stdscr.getmaxyx()
                curses.resizeterm(self.rows, self.cols)
                break
            try:
                c = self.getch()
                # stdscr.addstr(rows - 1, cols - 10, str(c))

                if c == -1:
                    continue
                if c in (curses.KEY_ENTER, 10, 13):
                    command = ''.join(keypresses[1:]).split(' ')
                    keypresses = []
                    break
                elif c == 127:
                    # Backspace
                    keypresses = keypresses[:-1]
                    break
                else:
                    if chr(c) == ':':
                        mode = 'command'
                    if mode == 'command':
                        keypresses.append(chr(c))
                    else:
                        if chr(c) == 't':
                            show = 'tasks'
                            i_offset = 0
                            break
                        elif chr(c) == 'r':
                            show = 'rules'
                            i_offset = 0
                            break
                        elif chr(c) == 'f':
                            show = 'files'
                            i_offset = 0
                            break
                        elif chr(c) == 'j':
                            i_offset -= 1
                            break
                        elif chr(c) == 'k':
                            if i_offset <= -1:
                                i_offset += 1
                            break
                        elif chr(c) == 'g':
                            i_offset = 0
                            break
                        elif chr(c) == 'G':
                            i_offset = -10000
                            break
                        elif chr(c) == 'w':
                            self.wrap = not self.wrap
                            break
                        elif chr(c) == 'R':
                            self.remake = load_remake(self.remake.name)
                            self.remake.task_ctrl.build_task_DAG()
                            self.monitor = RemakeMonitor(self.remake)
                            self.remake_sha1sum = sha1sum(Path(self.remake.name + '.py'))
                            break
                        elif chr(c) == 'F':
                            self.remake = load_remake(self.remake.name)
                            self.remake.finalize()
                            self.monitor = RemakeMonitor(self.remake)
                            self.remake_sha1sum = sha1sum(Path(self.remake.name + '.py'))
                            break

                stdscr.addstr(self.rows - 1, 0, ''.join(keypresses))
            except curses.error:
                pass
            stdscr.refresh()
        if command:
            if command[0] == 'q':
                raise Quit()
            elif command[0] == 'show':
                show = command[1]
                i_offset = 0
            elif command[0] == 'task':
                show = 'task'
                self.task_i = int(command[1])
                i_offset = 0
        return mode, command, keypresses, show, i_offset

    def monitor_loop(self):
        mode = None
        command = None
        keypresses = []
        show = 'tasks'
        i_offset = 0
        while True:
            # Monitor loop.
            self.monitor.refresh()
            if self.remake_sha1sum != sha1sum(Path(self.remake.name + '.py')):
                self.remake_name = self.remake.name + '*'
            else:
                self.remake_name = self.remake.name

            self.ui(mode, command, keypresses, show, self.remake_name)
            if mode == 'command':
                if command:
                    self.stdscr.addstr(self.rows - 1, 0, ':' + ' '.join(command))
                    command = None
                    mode = None
                elif keypresses:
                    self.stdscr.addstr(self.rows - 1, 0, ''.join(keypresses))

            self.summary(self.monitor.status_counts)
            if show == 'tasks':
                output = self.show_tasks()
            elif show == 'rules':
                output = self.show_rules()
            elif show == 'files':
                output = self.show_files()
            elif show == 'task':
                output = self.show_task(self.task_i)
            else:
                raise Exception(f'Unknown show: {show}')

            self.display_output(output, i_offset)

            try:
                (mode, command, keypresses,
                 show, i_offset) = self.input_loop(mode, command, keypresses, show, i_offset)
            except Quit:
                break


def remake_curses_monitor(stdscr, remake: Remake, timeout: float):
    _mon = RemakeMonitorCurses(stdscr, remake, timeout)
    _mon.monitor_loop()
