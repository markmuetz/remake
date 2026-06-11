import resource
import time

from remake import Remake, Sqlite3Backend, rule
from remake.core.dag import expand_rule

t0 = time.perf_counter()

@rule(
    inputs={'raw': 'data/raw/{a}/{b}.nc'},
    outputs={'out': 'data/out/{a}/{b}.nc'},
    matrix={'a': list(range(1000)), 'b': list(range(1000))},
)
def process(inputs, outputs, a, b):
    pass

rmk = Remake(rules=[process], metadata=Sqlite3Backend(':memory:'), check_outputs='never')
rmk.finalize()
t1 = time.perf_counter()
print(f'load+finalize:        {t1 - t0:8.3f} s')

tasks = expand_rule(process)
t2 = time.perf_counter()
print(f'expand 1e6 Tasks:     {t2 - t1:8.3f} s  ({len(tasks)} tasks)')

keys = [t.key for t in tasks]
t3 = time.perf_counter()
print(f'all keys (sha1):      {t3 - t2:8.3f} s')

n_in = sum(len(t.inputs) for t in tasks)
n_out = sum(len(t.outputs) for t in tasks)
t4 = time.perf_counter()
print(f'all inputs+outputs:   {t4 - t3:8.3f} s  ({n_in} inputs, {n_out} output tokens)')

runnable, deferred = rmk.plan()
t5 = time.perf_counter()
print(f'plan (never, empty DB): {t5 - t4:6.3f} s  ({len(runnable)} runnable)')

print(f'TOTAL:                {t5 - t0:8.3f} s')
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
print(f'peak RSS:             {rss:8.2f} GB')

# Baseline (2026-06-11, in-memory DB, local ext4):
#   load+finalize 0.001s; expand 1.4s; keys 2.6s; inputs+outputs 4.4s;
#   plan(never, empty DB) 7.5s; plan(fallback, empty DB) 22s (1e6 stats).
#   Peak RSS: 0.5 GB expand+keys, 2.1 GB fully materialised.
# Run manually: PYTHONPATH=src python tests/benchmarks/bench_million_tasks.py
# (not collected by pytest — no test_ prefix).
