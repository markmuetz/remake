"""Benchmark: planning cost of path-derived dependencies (review 2026-09-24
H3, core/deps.py) at the design scale — 1e4 upstream tasks x 100 outputs,
one downstream rule of 1e4 tasks each reading 100 upstream files.

`idiom` (inputs=up.outputs, shared matrix) is recognised from templates and
never resolves a path. `shifted` (a callable reading b-1) must resolve both
sides (~2e6 paths) — but only when an upstream task is rerunning or newer.

Run manually: PYTHONPATH=src python tests/benchmarks/bench_edge_map.py

Baseline (2026-09-25, M-series MacBook Air, in-memory DB):
  idiom:   plan all fresh 0.08 s, 1 upstream fail 0.08 s, backstop 0.09 s
  shifted: plan all fresh 0.07 s, 1 upstream fail 1.27 s, backstop 1.58 s
  peak RSS 0.65 GB
"""
import resource
import time

from remake import Remake, Sqlite3Backend, rule
from remake.core.dag import expand_rule
from remake.metadata import TASK_STATUS_SUCCESS, TASK_STATUS_FAILED
M = {'a': list(range(100)), 'b': list(range(100))}
OUT = {f'v{i}': f'data/out/{{a}}/{{b}}/var_{i}.nc' for i in range(100)}

@rule(outputs=OUT, matrix=M)
def up(outputs, a, b): pass

@rule(inputs=up.outputs, outputs={'o': 'data/idiom/{a}/{b}.nc'}, matrix=M, depends_on=[up])
def idiom(inputs, outputs, a, b): pass

def shifted_inputs(a, b):
    return {f'v{i}': f'data/out/{a}/{max(b - 1, 0)}/var_{i}.nc' for i in range(100)}

@rule(inputs=shifted_inputs, outputs={'o': 'data/shift/{a}/{b}.nc'}, matrix=M, depends_on=[up])
def shifted(inputs, outputs, a, b): pass

for down in (idiom, shifted):
    rmk = Remake(rules=[up, down], metadata=Sqlite3Backend(':memory:'), check_outputs='never')
    rmk.finalize()
    tasks = expand_rule(up) + expand_rule(down)
    rmk.metadata.update_tasks(tasks, TASK_STATUS_SUCCESS)
    t0 = time.perf_counter(); r, _ = rmk.plan(); t1 = time.perf_counter()
    print(f'{down.name}: plan all fresh      {t1 - t0:6.2f} s ({len(r)})')
    rmk.metadata.update_task(tasks[5], TASK_STATUS_FAILED)
    t0 = time.perf_counter(); r, _ = rmk.plan(); t1 = time.perf_counter()
    print(f'{down.name}: plan 1 upstream fail {t1 - t0:6.2f} s ({len(r)} runnable)')
    # backstop: upstream task newer
    rmk.metadata.begin_invocation()
    rmk.metadata.update_tasks([tasks[7]], TASK_STATUS_SUCCESS)
    t0 = time.perf_counter(); r, _ = rmk.plan(); t1 = time.perf_counter()
    print(f'{down.name}: plan backstop       {t1 - t0:6.2f} s ({len(r)} runnable)')
print(f'peak RSS {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9:.2f} GB')
