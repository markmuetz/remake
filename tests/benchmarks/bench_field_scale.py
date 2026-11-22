"""Benchmark at the *design* scale: 1e4 tasks x 100 files each = 1e6 files.

This is the shape real pipelines take (design doc, "Scale target"): task
counts around 1e4 are what SLURM arrays and field remakefiles actually
reach; the million is in *files*, so the per-file paths — input/output
resolution (token construction) and the stat-heavy check_outputs modes —
are the scaling frontier, not the per-task DB/planner loop that
bench_million_tasks.py exercises.

Run manually: PYTHONPATH=src python tests/benchmarks/bench_field_scale.py
(not collected by pytest — no test_ prefix).

Pass ``--json PATH`` to also write machine-readable timings (phase ->
seconds, plus a ``calibration`` pure-Python loop time and run metadata);
CI feeds that to ``check_regression.py``. The full size takes ~2 s, so
there is no reduced-size option.
"""
import argparse
import json
import platform
import resource
import sys
import time

CALIBRATION_ITERS = 2_000_000


def calibrate():
    """Time a fixed pure-Python loop (best of 5), as a runner-speed yardstick."""
    best = float('inf')
    for _ in range(5):
        t = time.perf_counter()
        x = 0
        for i in range(CALIBRATION_ITERS):
            x += i * i % 7
        best = min(best, time.perf_counter() - t)
    return best


_parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
_parser.add_argument('--json', metavar='PATH', help='write phase timings as JSON')
_args = _parser.parse_args()
calibration = calibrate()

from remake import Remake, Sqlite3Backend, rule
from remake.core.dag import expand_rule
from remake.metadata import TASK_STATUS_SUCCESS

N_TASKS = 10_000  # 100 a x 100 b
N_FILES_PER_TASK = 100

t0 = time.perf_counter()

# 100 outputs per task, path templated by both kwargs — the fan-out-per-task
# shape (e.g. one task writing one file per variable/level).
OUTPUTS = {
    f'v{i}': f'data/out/{{a}}/{{b}}/var_{i}.nc' for i in range(N_FILES_PER_TASK)
}


@rule(
    inputs={'raw': 'data/raw/{a}/{b}.nc'},
    outputs=OUTPUTS,
    matrix={'a': list(range(100)), 'b': list(range(100))},
)
def process(inputs, outputs, a, b):
    pass


rmk = Remake(rules=[process], metadata=Sqlite3Backend(':memory:'), check_outputs='never')
rmk.finalize()
t1 = time.perf_counter()
print(f'load+finalize:            {t1 - t0:8.3f} s')

tasks = expand_rule(process)
t2 = time.perf_counter()
print(f'expand {len(tasks)} Tasks:        {t2 - t1:8.3f} s')

n_out = sum(len(t.outputs) for t in tasks)
t3 = time.perf_counter()
print(f'resolve all outputs:      {t3 - t2:8.3f} s  ({n_out} tokens)')

runnable, deferred = rmk.plan()
t4 = time.perf_counter()
print(f'plan (never, empty DB):   {t4 - t3:8.3f} s  ({len(runnable)} runnable)')

# The stat frontier: fallback mode stats every declared output of every
# no-record task — 1e6 stats. Local-filesystem numbers flatter this badly:
# on Lustre/NFS each stat is a round trip, so multiply by ~10-100x for the
# cluster reality this guards against.
rmk.check_outputs = 'fallback'
runnable, deferred = rmk.plan()
t5 = time.perf_counter()
print(f'plan (fallback, 1e6 stats): {t5 - t4:6.3f} s  ({len(runnable)} runnable)')
rmk.check_outputs = 'never'

rmk.metadata.update_tasks(tasks, TASK_STATUS_SUCCESS)
t6 = time.perf_counter()
print(f'record {len(tasks)} completions: {t6 - t5:8.3f} s  (one batched txn)')

runnable, deferred = rmk.plan()
t7 = time.perf_counter()
print(f'plan (populated DB):      {t7 - t6:8.3f} s  ({len(runnable)} runnable)')

print(f'TOTAL:                    {t7 - t0:8.3f} s')
# ru_maxrss is bytes on macOS, kilobytes on Linux.
_rss_div = 1e9 if sys.platform == 'darwin' else 1e6
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / _rss_div
print(f'peak RSS:                 {rss:8.2f} GB')

if _args.json:
    result = {
        'calibration': calibration,
        'phases': {
            'load_finalize': t1 - t0,
            'expand': t2 - t1,
            'resolve_outputs': t3 - t2,
            'plan_never_empty': t4 - t3,
            'plan_fallback_stats': t5 - t4,
            'record_completions': t6 - t5,
            'plan_populated': t7 - t6,
            'total': t7 - t0,
        },
        'meta': {
            'python': platform.python_version(),
            'machine': f'{platform.system()} {platform.machine()}',
            'n_tasks': N_TASKS,
            'n_files_per_task': N_FILES_PER_TASK,
            'peak_rss_gb': rss,
        },
    }
    with open(_args.json, 'w') as f:
        json.dump(result, f, indent=2)

# Baseline (2026-07-02, in-memory DB, local ext4, post storage/query rework):
#   load+finalize 0.006s; expand 1e4 tasks 0.025s;
#   resolve 1e6 output tokens 2.1s / 0.63 GB peak;
#   plan(never, empty DB) 0.07s; plan(fallback, 1e6 stats) 2.3s;
#   record 1e4 completions (one txn) 0.08s; plan(populated) 0.27s.
# Reading: per-task costs are trivial at the design scale — the two
# dominant lines are per-FILE (token resolution and the stat sweep), and
# the stat number is the local-ext4 best case.
