# tests/benchmarks/bench_sqlite_contention.py
#
# Stress-test Sqlite3Backend.update_task()'s retry_lock_commit locking under
# heavy concurrency, driven directly from a raw SLURM array job -- no
# Remake/Rule/planner/Executor involved, just `Sqlite3Backend` (the class
# that actually owns the locking code) plus minimal duck-typed Rule/Task
# stand-ins.
#
# ex4 (examples/ex4_zarr_slurm.py) gave one data point: a 176-element array
# where each element did several seconds of real work, with zero lock
# errors. This script instead fires N_ARRAY processes that do nothing but
# hammer update_task() in a tight loop, to find where retry_lock_commit's
# unbounded exponential backoff (see sqlite3_backend.py) actually kicks in
# on shared NFS.
#
# Usage on JASMIN:
#
#   # 1. Create a fresh db + sbatch script in a scratch dir.
#   uv run python tests/benchmarks/bench_sqlite_contention.py setup \
#       --dir /tmp/sqlite_stress --n-array 500 --n-updates 50
#
#   # 2. Submit the array job.
#   cd /tmp/sqlite_stress && sbatch stress.sbatch
#
#   # 3. After it completes, summarise timings and scan for lock errors.
#   uv run python tests/benchmarks/bench_sqlite_contention.py report \
#       --dir /tmp/sqlite_stress
#
# Not collected by pytest (no test_ prefix); run manually.

import argparse
import json
import random
import time
from pathlib import Path

from remake.metadata.sqlite3_backend import Sqlite3Backend

REPO_ROOT = Path(__file__).resolve().parents[2]

SBATCH_TPL = """\
#!/bin/bash
#SBATCH --job-name=sqlite-stress
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH --account={account}
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --array=0-{max_index}
#SBATCH --output={dir}/logs/%a.out
#SBATCH --error={dir}/logs/%a.err

uv run --project {repo_root} python {script} worker \\
    --dir {dir} --n-updates {n_updates} --n-updates-max {n_updates_max} \\
    --task-id $SLURM_ARRAY_TASK_ID
"""


class FakeRule:
    """Duck-types the bits of `Rule` that Sqlite3Backend touches."""

    def __init__(self, name):
        self.name = name
        self.uses = {}
        self.source = {'inputs': '', 'outputs': '', 'run': f'def {name}(): pass'}


class FakeTask:
    """Duck-types the bits of `Task` that update_task() touches."""

    def __init__(self, key, rule):
        self.key = key
        self.rule = rule


RULE_NAME = 'stress'


def cmd_setup(args):
    dir_ = Path(args.dir)
    (dir_ / 'logs').mkdir(parents=True, exist_ok=True)
    (dir_ / 'results').mkdir(parents=True, exist_ok=True)

    db_path = dir_ / 'remake.db'
    if db_path.exists():
        db_path.unlink()
    backend = Sqlite3Backend(dbloc=db_path)
    backend.ensure_rules([FakeRule(RULE_NAME)])

    sbatch = SBATCH_TPL.format(
        account=args.account,
        time=args.time,
        mem=args.mem,
        max_index=args.n_array - 1,
        dir=dir_.resolve(),
        repo_root=REPO_ROOT,
        script=Path(__file__).resolve(),
        n_updates=args.n_updates,
        n_updates_max=args.n_updates_max,
    )
    (dir_ / 'stress.sbatch').write_text(sbatch)
    print(f'Wrote {db_path} and {dir_ / "stress.sbatch"}')
    print(f'Next: cd {dir_} && sbatch stress.sbatch')


def cmd_worker(args):
    dir_ = Path(args.dir)
    backend = Sqlite3Backend(dbloc=dir_ / 'remake.db')
    backend.ensure_rules([FakeRule(RULE_NAME)])
    rule = FakeRule(RULE_NAME)

    # Random per-worker call count: spreads finish times across overlapping
    # waves instead of one synchronised burst-then-done, closer to a
    # heterogeneous real rule array.
    n_updates = random.randint(args.n_updates, args.n_updates_max)

    durations = []
    for i in range(n_updates):
        key = f'{args.task_id:05d}_{i:05d}'
        task = FakeTask(key=key, rule=rule)
        t0 = time.perf_counter()
        backend.update_task(task, status=0)
        durations.append(time.perf_counter() - t0)

    result_path = dir_ / 'results' / f'{args.task_id:05d}.json'
    result_path.write_text(json.dumps({
        'task_id': args.task_id,
        'n_updates': n_updates,
        'durations': durations,
    }))


def cmd_report(args):
    dir_ = Path(args.dir)
    result_files = sorted((dir_ / 'results').glob('*.json'))
    all_durations = []
    for f in result_files:
        all_durations.extend(json.loads(f.read_text())['durations'])

    print(f'{len(result_files)} result files, {len(all_durations)} update_task() calls')
    if all_durations:
        slow = [d for d in all_durations if d > args.slow_threshold]
        print(f'  min={min(all_durations):.4f}s  max={max(all_durations):.4f}s  '
              f'mean={sum(all_durations) / len(all_durations):.4f}s')
        print(f'  {len(slow)} calls > {args.slow_threshold}s '
              f'(likely retried at least once)')

    lock_errors = []
    for f in sorted((dir_ / 'logs').glob('*.err')):
        for line in f.read_text().splitlines():
            if 'OperationalError' in line or 'database is locked' in line:
                lock_errors.append((f.name, line))

    print(f'{len(lock_errors)} OperationalError/lock lines across logs/*.err')
    for fname, line in lock_errors[:20]:
        print(f'  {fname}: {line}')

    db_path = dir_ / 'remake.db'
    backend = Sqlite3Backend(dbloc=db_path)
    (count,) = backend.conn.execute('SELECT COUNT(*) FROM task').fetchone()
    print(f'task table row count: {count}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_setup = sub.add_parser('setup')
    p_setup.add_argument('--dir', required=True)
    p_setup.add_argument('--n-array', type=int, default=500)
    p_setup.add_argument('--n-updates', type=int, default=50)
    p_setup.add_argument('--n-updates-max', type=int, default=150)
    p_setup.add_argument('--account', default='afesp')
    p_setup.add_argument('--time', default='00:05:00')
    p_setup.add_argument('--mem', default='256M')
    p_setup.set_defaults(func=cmd_setup)

    p_worker = sub.add_parser('worker')
    p_worker.add_argument('--dir', required=True)
    p_worker.add_argument('--n-updates', type=int, required=True)
    p_worker.add_argument('--n-updates-max', type=int, required=True)
    p_worker.add_argument('--task-id', type=int, required=True)
    p_worker.set_defaults(func=cmd_worker)

    p_report = sub.add_parser('report')
    p_report.add_argument('--dir', required=True)
    p_report.add_argument('--slow-threshold', type=float, default=1.0)
    p_report.set_defaults(func=cmd_report)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
