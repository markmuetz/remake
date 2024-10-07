from collections import defaultdict

import numpy as np
import pandas as pd
from tabulate import tabulate, SEPARATING_LINE


class GlobalTimer:
    global_timers = {}
    active_timer = None

    def __init__(self, name):
        self.name = str(name)
        self.timers = defaultdict(list)
        self.last_time = None
        self.curr_key = None
        self.curr_level = 1
        self.last_time_at_level_stack = []
        self.descriptions = {}
        self.internal_time = []

    def __call__(self, key, description=''):
        call_time = pd.Timestamp.now()
        GlobalTimer.active_timer = self
        level = len(str(key).split('.'))
        combined_level = max(len(str(k).split('.')) for k in [self.curr_key, key])

        time = pd.Timestamp.now()
        if self.curr_key is not None:
            self.timers[(self.curr_key, key, combined_level)].append(time - self.last_time)
        self.last_time = time

        self.descriptions[(key, combined_level)] = description
        if level > self.curr_level:
            self.last_time_at_level_stack.append((self.curr_key, self.last_time))
        elif level < self.curr_level:
            last_key, last_time_at_level = self.last_time_at_level_stack.pop()
            if self.curr_key is not None:
                self.timers[(last_key, key, level)].append(time - last_time_at_level)

        self.curr_level = level
        self.curr_key = key
        self.internal_time.append(pd.Timestamp.now() - call_time)

    def output(self, max_level=None):
        output = []
        def timer_sorter(item):
            k1, k2, level = item[0]
            return str(k1), level

        sorted_timers = dict(sorted(self.timers.items(), key=timer_sorter))
        total_time = 0
        for key in sorted_timers.keys():
            k1, k2, combined_level = key
            if combined_level > 1:
                continue
            timers = self.timers[key]
            times = [t.total_seconds() for t in timers]
            total_time += np.sum(times)

        for key in sorted_timers.keys():
            k1, k2, combined_level = key
            if max_level and combined_level > max_level:
                continue
            level = len(str(k1).split('.'))
            description = self.descriptions.get((k1, level), '')
            indent = (combined_level - 1) * '--'

            timers = self.timers[key]
            times = [t.total_seconds() for t in timers]
            time_total = np.sum(times)
            time_mean = np.mean(times)
            time_std = np.std(times)
            percent = time_total / total_time * 100
            count = len(times)
            output.append((f'{indent}{k1} -> {k2}', f'{time_total:.2g}s', f'{percent:04.1f}%', f'{time_mean:.2g}s', f'(+/- {time_std:.2g}s)', f'{count}', description))

        internal_times = [t.total_seconds() for t in self.internal_time]
        output.append(SEPARATING_LINE)
        output.append(('total', f'{total_time:.2g}s', '', '', '', '', ''))
        output.append(SEPARATING_LINE)
        output.append(('internal', f'{np.sum(internal_times):.2g}s', '', '', '', '', ''))
        return f'{self.name}\n' + '=' * len(self.name) + '\n' + tabulate(output, headers=('tx', 'total', 'percent', 'mean', 'std', 'count', 'desc'))

    def __str__(self):
        return self.output()

    def reset(self):
        self.timers = defaultdict(list)
        self.last_time = None
        self.curr_key = None


def get_global_timer(name=None):
    if name is None:
        if not GlobalTimer.active_timer:
            global_timer = GlobalTimer('root')
            GlobalTimer.global_timers['root'] = global_timer
            GlobalTimer.active_timer = global_timer
        return GlobalTimer.active_timer
    elif name in GlobalTimer.global_timers:
        return GlobalTimer.global_timers[name]
    else:
        global_timer = GlobalTimer(name)
        GlobalTimer.global_timers[name] = global_timer
        return global_timer

