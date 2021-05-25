import sys
from collections import Counter
import datetime as dt
from pathlib import Path

import curses
from curses import wrapper

from remake import Remake
from remake.load_remake import load_remake
from remake.metadata import METADATA_VERSION

class RemakeMonitor:
    def __init__(self, remake):
        self.remake = remake
        self.status_dir = Path(remake.task_ctrl.dotremake_dir / METADATA_VERSION /
                               remake.name / 'task_status')

    def refresh(self):
        paths = self.status_dir.glob('*/*.status')
        self.status_counts = Counter()
        self.task_key_status_map = {}

        for path in paths:
            time, status = path.read_text().split('\n')[-2].split(';')
            self.status_counts[status] += 1
            self.task_key_status_map[f'{path.parts[-2]}{path.stem}'] = status

        self.statuses = [(t, self.task_key_status_map[t.path_hash_key()])
                         for t in self.remake.task_ctrl.sorted_tasks
                         if t.path_hash_key() in self.task_key_status_map]



def remake_curses_monitor(stdscr, remake: Remake, timeout: int):
    monitor = RemakeMonitor(remake)
    rows, cols = stdscr.getmaxyx()
    colour_pairs = {
        "CANNOT_RUN": 1,
        "PENDING": 2,
        "REMAINING": 3,
        "RUNNING": 4,
        "COMPLETED": 5,
        "ERROR": 6,
    }
    curses.init_pair(colour_pairs['CANNOT_RUN'], curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['PENDING'], curses.COLOR_MAGENTA, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['REMAINING'], curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['RUNNING'], curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['COMPLETED'], curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['ERROR'], curses.COLOR_RED, curses.COLOR_BLACK)
    stdscr.nodelay(True)
    input_loop_timeout = 10
    num_input_loops = timeout // input_loop_timeout

    curses.curs_set(0)
    command = None
    keypresses = []
    mode = None
    show = 'tasks'
    while True:
        # Refresh loop.
        monitor.refresh()

        stdscr.clear()
        if mode == 'command':
            if command:
                stdscr.addstr(rows - 2, 0, ':' + ' '.join(command))
                command = None
                mode = None
            elif keypresses:
                stdscr.addstr(rows - 1, 0, ''.join(keypresses))

        timestr = f'Time     : {dt.datetime.now().replace(microsecond=0)}'
        stdscr.addstr(0, cols // 2 - len(timestr) // 2, timestr)
        status_counts = monitor.status_counts
        stdscr.addstr(1, 0, f'Cant run : {status_counts["CANNOT_RUN"]}', curses.color_pair(colour_pairs['CANNOT_RUN']))
        stdscr.addstr(2, 0, f'Pending  : {status_counts["PENDING"]}', curses.color_pair(colour_pairs['PENDING']))
        stdscr.addstr(3, 0, f'Remaining: {status_counts["REMAINING"]}', curses.color_pair(colour_pairs['REMAINING']))
        stdscr.addstr(4, 0, f'Running  : {status_counts["RUNNING"]}', curses.color_pair(colour_pairs['RUNNING']))
        stdscr.addstr(5, 0, f'Completed: {status_counts["COMPLETED"]}', curses.color_pair(colour_pairs['COMPLETED']))
        stdscr.addstr(6, 0, f'Error    : {status_counts["ERROR"]}', curses.color_pair(colour_pairs['ERROR']))

        if show == 'tasks':
            for i, (task, status) in enumerate(monitor.statuses):
                stdscr.addstr(1 + i, 15, f'{str(i):>3}')
                stdscr.addstr(1 + i, 19, f'{status:<10}: {task}'[:cols - 19], curses.color_pair(colour_pairs[status]))
        elif show == 'rules':
            stdscr.addstr(1, 36, ' CR,  P, RM,  R,  C,  E')
            for i, rule in enumerate(remake.rules):
                stdscr.addstr(2 + i, 15, f'{str(rule.__name__)[:20]:<20}')
                rule_status = Counter([monitor.task_key_status_map[t.path_hash_key()]
                                      for t in rule.tasks
                                      if t in monitor.task_key_status_map])
                for j, status in enumerate(['CANNOT_RUN', 'PENDING', 'REMAINING', 'RUNNING', 'COMPLETED', 'ERROR']):
                    if status in rule_status:
                        stdscr.addstr(2 + i, 36 + j * 4, f'{str(rule_status[status]):>3}', curses.color_pair(colour_pairs[status]))
                    else:
                        stdscr.addstr(2 + i, 36 + j * 4, f'  0', curses.color_pair(colour_pairs[status]))
                    if status != 'ERROR':
                        stdscr.addstr(2 + i, 36 + j * 4 + 3, ',')
        elif show == 'files':
            paths = [p
                     for t in remake.task_ctrl.sorted_tasks
                     for p in t.outputs.values()]
            for i, path in enumerate(paths):
                stdscr.addstr(1 + i, 15, f'{path.exists():>5}: {path}')

        for i in range(num_input_loops):
            # Input loop.
            curses.napms(input_loop_timeout)
            try:
                c = stdscr.getch()
                if c == -1:
                    continue
                if c in (curses.KEY_ENTER, 10, 13):
                    stdscr.addstr(rows - 3, 0, 'ENTER')
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
                        try:
                            keypresses.append(chr(c))
                        except:
                            pass
                stdscr.addstr(rows - 1, 0, ''.join(keypresses))
            except curses.error:
                pass
            stdscr.refresh()
        if command:
            if command[0] == 'quit':
                break
            elif command[0] == 'show':
                show = command[1]


if __name__ == '__main__':
    remake = load_remake(sys.argv[1])
    remake.build_task_DAG()

    if len(sys.argv) == 3:
        wrapper(remake_curses_monitor, remake, int(sys.argv[1]))
    else:
        wrapper(remake_curses_monitor, remake)
