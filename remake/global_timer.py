from collections import defaultdict

import numpy as np
import pandas as pd
from tabulate import tabulate


class GlobalTimer:
    global_timers = {}
    active_timer = None

    def __init__(self, name):
        self.name = str(name)
        self.timers = defaultdict(list)
        self.last_time = None
        self.curr_key = None
        self.sub_timers = []
        self.parent_timer = None

    def __call__(self, key):
        time = pd.Timestamp.now()
        if self.curr_key is not None:
            self.timers[(self.curr_key, key)].append(time - self.last_time)

        self.curr_key = key
        self.last_time = pd.Timestamp.now()

    def __str__(self):
        output = []

        for k in self.timers.keys():
            k1, k2 = k
            timers = self.timers[k]
            # print(timers)
            times = [t.total_seconds() for t in timers]
            # print(times)
            time_total = np.sum(times)
            time_mean = np.mean(times)
            time_std = np.std(times)
            count = len(times)
            output.append((f'{k1} -> {k2}', f'{time_total:.2g}s', f'{time_mean:.2g}s', f'(+/- {time_std:.2g}s)', f'{count}'))
        return f'{self.name}\n' + '=' * len(self.name) + '\n' + tabulate(output, headers=('tx', 'total', 'mean', 'std', 'count'))

    def start(self):
        # TODO:
        raise NotImplemented
        if not GlobalTimer.active_timer:
            GlobalTimer.active_timer = self
        else:
            if self not in GlobalTimer.active_timer.sub_timers:
                GlobalTimer.active_timer.sub_timers.append(self)
            self.parent_timer = GlobalTimer.active_timer
            GlobalTimer.active_timer = self
        self('__start__')

    def end(self):
        # TODO:
        raise NotImplemented
        if GlobalTimer.active_timer == self:
            if self.parent_timer:
                GlobalTimer.active_timer = self.parent_timer
                self.parent_timer = None
            else:
                GlobalTimer.active_timer = None
        else:
            raise Exception('Did you forget to start or end a timer?')
        self('__end__')


    def reset(self):
        self.timers = defaultdict(list)
        self.last_time = None
        self.curr_key = None


def get_global_timer(name):
    if name in GlobalTimer.global_timers:
        return GlobalTimer.global_timers[name]
    else:
        global_timer = GlobalTimer(name)
        GlobalTimer.global_timers[name] = global_timer
        return global_timer

