import sys
from collections import Counter
import datetime as dt
from pathlib import Path

import curses
from curses import wrapper

from remake import Remake
from remake.load_remake import load_remake
from remake.metadata import METADATA_VERSION
from remake.util import sha1sum

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
            task_status_path = self.status_dir / key[:2] / (key[2:] + '.status')
            if not task_status_path.exists():
                status = 'UNKNOWN'
            else:
                time, status = task_status_path.read_text().split('\n')[-2].split(';')
            self.status_counts[status] += 1
            self.task_key_status_map[key] = status
            self.statuses.append((task, status))


def remake_curses_monitor(stdscr, remake: Remake, timeout: int):
    monitor = RemakeMonitor(remake)
    remake_sha1sum = sha1sum(Path(remake.name + '.py'))
    rows, cols = stdscr.getmaxyx()
    colour_pairs = {
        "CANNOT_RUN": 1,
        "PENDING": 2,
        "REMAINING": 3,
        "RUNNING": 4,
        "COMPLETED": 5,
        "ERROR": 6,
        "UNKNOWN": 7,
    }
    curses.init_pair(colour_pairs['CANNOT_RUN'], curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['PENDING'], curses.COLOR_MAGENTA, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['REMAINING'], curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['RUNNING'], curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['COMPLETED'], curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['ERROR'], curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(colour_pairs['UNKNOWN'], curses.COLOR_YELLOW, curses.COLOR_BLACK)

    curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_WHITE)
    stdscr.nodelay(True)
    input_loop_timeout = 10
    num_input_loops = timeout // input_loop_timeout

    curses.curs_set(0)
    command = None
    keypresses = []
    mode = None
    show = 'tasks'
    i_offset = 0
    while True:
        # Refresh loop.
        monitor.refresh()
        if remake_sha1sum != sha1sum(Path(remake.name + '.py')):
            remake_name = remake.name + '*'
        else:
            remake_name = remake.name

        stdscr.clear()
        if mode == 'command':
            if command:
                stdscr.addstr(rows - 2, 0, ':' + ' '.join(command))
                command = None
                mode = None
            elif keypresses:
                stdscr.addstr(rows - 1, 0, ''.join(keypresses))

        timestr = f'{dt.datetime.now().replace(microsecond=0)}'
        stdscr.addstr(0, cols // 2 - len(timestr) // 2, timestr)

        topline = [' '] * cols
        show_str = f' {show} '
        topline[cols // 2 - len(show_str): len(show_str)] = list(show_str)
        stdscr.addstr(1, 0, ''.join(topline), curses.color_pair(8))

        bottomline = [' '] * cols
        bottomline[:len(remake_name)] = list(remake_name)
        stdscr.addstr(rows - 2, 0, ''.join(bottomline), curses.color_pair(8))

        status_counts = monitor.status_counts
        stdscr.addstr(2, 0, f'Cant run : {status_counts["CANNOT_RUN"]}', curses.color_pair(colour_pairs['CANNOT_RUN']))
        stdscr.addstr(3, 0, f'Unknown  : {status_counts["UNKNOWN"]}', curses.color_pair(colour_pairs['UNKNOWN']))
        stdscr.addstr(4, 0, f'Remaining: {status_counts["REMAINING"]}', curses.color_pair(colour_pairs['REMAINING']))
        stdscr.addstr(5, 0, f'Pending  : {status_counts["PENDING"]}', curses.color_pair(colour_pairs['PENDING']))
        stdscr.addstr(6, 0, f'Running  : {status_counts["RUNNING"]}', curses.color_pair(colour_pairs['RUNNING']))
        stdscr.addstr(7, 0, f'Completed: {status_counts["COMPLETED"]}', curses.color_pair(colour_pairs['COMPLETED']))
        stdscr.addstr(8, 0, f'Error    : {status_counts["ERROR"]}', curses.color_pair(colour_pairs['ERROR']))

        if show == 'tasks':
            if i_offset < -len(monitor.statuses) + rows - 4 or i_offset == -10000:
                i_offset = -len(monitor.statuses) + rows - 4
            if i_offset > 0:
                i_offset = 0
            for i, (task, status) in enumerate(monitor.statuses):
                if 2 + i + i_offset <= 1:
                    continue
                if 2 + i + i_offset >= rows - 2:
                    break
                stdscr.addstr(2 + i + i_offset, 15, f'{str(i):>3}')
                stdscr.addstr(2 + i + i_offset, 19, f'{status:<10}: {task}'[:cols - 19], curses.color_pair(colour_pairs[status]))
        elif show == 'rules':
            if i_offset < -len(remake.rules) + 1 + rows - 4 or i_offset == -10000:
                i_offset = -len(remake.rules) + 1 + rows - 4
            if i_offset > 0:
                i_offset = 0
            stdscr.addstr(2, 36, ' CR,  P, RM,  R,  C,  E')
            for i, rule in enumerate(remake.rules):
                if 2 + i + i_offset <= 1:
                    continue
                if 2 + i + i_offset >= rows - 2:
                    break
                stdscr.addstr(3 + i + i_offset, 15, f'{str(rule.__name__)[:20]:<20}')
                rule_status = Counter([monitor.task_key_status_map[t.path_hash_key()]
                                      for t in rule.tasks
                                      if t.path_hash_key() in monitor.task_key_status_map])
                for j, status in enumerate(['CANNOT_RUN', 'PENDING', 'REMAINING', 'RUNNING', 'COMPLETED', 'ERROR']):
                    if status in rule_status:
                        stdscr.addstr(3 + i + i_offset, 36 + j * 4, f'{str(rule_status[status]):>3}', curses.color_pair(colour_pairs[status]))
                    else:
                        stdscr.addstr(3 + i + i_offset, 36 + j * 4, f'  0', curses.color_pair(colour_pairs[status]))
                    if status != 'ERROR':
                        stdscr.addstr(3 + i + i_offset, 36 + j * 4 + 3, ',')
        elif show == 'files':
            paths = [p
                     for t in remake.task_ctrl.sorted_tasks
                     for p in t.outputs.values()]
            if i_offset < -len(paths) + rows - 4 or i_offset == -10000:
                i_offset = -len(paths) + rows - 4
            if i_offset > 0:
                i_offset = 0
            for i, path in enumerate(paths):
                if 2 + i + i_offset <= 1:
                    continue
                if 2 + i + i_offset >= rows - 2:
                    break
                if path.exists():
                    stdscr.addstr(2 + i + i_offset, 15, f'{str(path.exists()):>5}: {path}', curses.color_pair(colour_pairs['COMPLETED']))
                else:
                    stdscr.addstr(2 + i + i_offset, 15, f'{str(path.exists()):>5}: {path}')

        for i in range(num_input_loops):
            # Input loop.
            curses.napms(input_loop_timeout)
            try:
                c = stdscr.getch()
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
                        try:
                            keypresses.append(chr(c))
                        except:
                            pass
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
                        elif chr(c) == 'R':
                            remake = load_remake(remake.name)
                            remake.task_ctrl.build_task_DAG()
                            monitor = RemakeMonitor(remake)
                            remake_sha1sum = sha1sum(Path(remake.name + '.py'))

                stdscr.addstr(rows - 1, 0, ''.join(keypresses))
            except curses.error:
                pass
            stdscr.refresh()
        if command:
            if command[0] == 'q':
                break
            elif command[0] == 'show':
                show = command[1]
                i_offset = 0


if __name__ == '__main__':
    remake = load_remake(sys.argv[1])
    remake.build_task_DAG()

    if len(sys.argv) == 3:
        wrapper(remake_curses_monitor, remake, int(sys.argv[1]))
    else:
        wrapper(remake_curses_monitor, remake)

