import sys
from collections import Counter
import datetime as dt
from pathlib import Path

import curses
from curses import wrapper

from remake import Remake
from remake.load_remake import load_remake
from remake.metadata import METADATA_VERSION


def remake_curses_monitor(stdscr, remake: Remake, timeout: int):
    rows, cols = stdscr.getmaxyx()
    status_dir = Path(remake.task_ctrl.dotremake_dir / METADATA_VERSION / remake.name / 'task_status')
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

    # task_ctrl = gen_task_ctrl(False)
    curses.curs_set(0)
    while True:
        stdscr.clear()

        paths = status_dir.glob('*/*.status')
        status_counts = Counter()
        statuses = {}
        running = []
        for path in paths:
            time, status = path.read_text().split('\n')[-2].split(';')
            status_counts[status] += 1
            statuses[f'{path.parts[-2]}{path.stem}'] = status
            if status == 'RUNNING':
                running.append(f'{path.parts[-2]}{path.stem}')
        timestr = f'Time     : {dt.datetime.now().replace(microsecond=0)}'
        stdscr.addstr(0, cols // 2 - len(timestr) // 2, timestr)
        stdscr.addstr(1, 0, f'Cant run : {status_counts["CANNOT_RUN"]}', curses.color_pair(colour_pairs['CANNOT_RUN']))
        stdscr.addstr(2, 0, f'Pending  : {status_counts["PENDING"]}', curses.color_pair(colour_pairs['PENDING']))
        stdscr.addstr(3, 0, f'Remaining: {status_counts["REMAINING"]}', curses.color_pair(colour_pairs['REMAINING']))
        stdscr.addstr(4, 0, f'Running  : {status_counts["RUNNING"]}', curses.color_pair(colour_pairs['RUNNING']))
        stdscr.addstr(5, 0, f'Completed: {status_counts["COMPLETED"]}', curses.color_pair(colour_pairs['COMPLETED']))
        stdscr.addstr(6, 0, f'Error    : {status_counts["ERROR"]}', curses.color_pair(colour_pairs['ERROR']))

        for i, task in enumerate(remake.task_ctrl.sorted_tasks):
            if task.path_hash_key() in statuses:
                status = statuses[task.path_hash_key()]
                stdscr.addstr(1 + i, 15, f'{status:<10}: {task}'[:cols - 15], curses.color_pair(colour_pairs[status]))

        stdscr.refresh()
        curses.napms(timeout)


if __name__ == '__main__':
    remake = load_remake(sys.argv[1])
    remake.build_task_DAG()

    if len(sys.argv) == 3:
        wrapper(remake_curses_monitor, remake, int(sys.argv[1]))
    else:
        wrapper(remake_curses_monitor, remake)
