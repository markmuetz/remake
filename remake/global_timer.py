from collections import defaultdict

import numpy as np
import pandas as pd
from tabulate import tabulate


class GlobalTimer:
    global_timers = {}

    def __init__(self, name):
        self.name = name
        self.timers = defaultdict(list)
        self.last_time = None
        self.curr_key = None

    def __call__(self, key):
        time = pd.Timestamp.now()
        if self.curr_key is not None:
            self.timers[(self.curr_key, key)].append(time - self.last_time)

        self.last_time = time
        self.curr_key = key

    def __str__(self):
        output = []

        for k in self.timers.keys():
            k1, k2 = k
            timers = self.timers[k]
            times_ms = [t.microseconds for t in timers]
            time_mean_ms = np.mean(times_ms)
            time_std_ms = np.std(times_ms)
            output.append((f'{k1} -> {k2}', f'{time_mean_ms / 1e6:.2g}s', f'(+/- {time_std_ms / 1e6:.2g}s)'))
        return f'{self.name}\n' + tabulate(output, headers=('tx', 'mean', 'std'))


def get_global_timer(name):
    if name in GlobalTimer.global_timers:
        return GlobalTimer.global_timers[name]
    else:
        global_timer = GlobalTimer(name)
        GlobalTimer.global_timers[name] = global_timer
        return global_timer

