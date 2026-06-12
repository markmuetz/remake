# Per-task logging

Design for fixing the shared-log corruption found during JASMIN validation
(2026-06-12), and the layout of per-task log files. See todos.md (Smaller
debts) for the original observation and discussion.md for the related
`.remake` layout question.

## Problem

Two related defects in the current logging:

1. **Concurrent corruption.** Every remakefile subcommand appends to the
   single `.remake/remake.log`. Under a SLURM array job each element is a
   separate `remake run-array-task` process; 176 concurrent writers on
   JASMIN's NFS produced garbled rule names and mid-line timestamps. The
   loguru file sink is not safe for concurrent writers across nodes (no
   cross-node locking). SQLite (`.remake/remake.db`) was unaffected by the
   same load — only the log file corrupts.

2. **Index-named SLURM output is unstable.** The scheduler's own
   `.remake/slurm/output/<rule>/%a.out`/`.err` files are named by array
   index, and indices are *not* stable: each run rewrites
   `.remake/jobs/<rule>.json` with whatever subset needs running, so
   `proc/3.out` from one run and the next are different tasks, and reruns
   silently overwrite a previous task's output.

## Design

Task keys are the stable per-task identifier (40-char hex sha1 of
`rule_name:kwargs`, identical across runs). Per-task processes log to a
key-named file instead of the shared log:

```
.remake/tasks/log/<rule>/<key[:2]>/<key[2:]>.log
```

- **Who writes it**: `remake run-task` and `remake run-array-task` — the
  per-task-process entry points (SLURM array elements, and later multiproc
  workers). They do **not** add the shared `.remake/remake.log` sink at
  all; one process, one file, no concurrent writers.
- **Everything else is unchanged**: `run`, `info`, `resubmit` etc. keep the
  shared `.remake/remake.log` sink. A single-process `remake run`
  (singleproc executor) has no concurrency problem and its interleaved
  whole-run log is the more useful artefact there.
- **Overwrite, not append** (`mode='w'`): "the log" for a task is
  unambiguous — the latest attempt. Failure history is the metadata DB's
  job (status, timestamp, full traceback are already stored per task).
- **SLURM `.out`/`.err` stay index-named.** sbatch can only template
  scheduler-known values (`%a`, `%A`, `%j`) into output paths — a task key
  cannot be computed in an `#SBATCH -o` line. They remain ephemeral
  scheduler droppings; the `echo "SLURM RUNNING <rule> <index>"` line and
  the `task_key` field in `.remake/jobs/<rule>.json` cross-reference index
  to key when needed.

### Why sharded (`<key[:2]>/<key[2:]>`)

Directory entry counts matter on JASMIN-class filesystems. Sharding on the
first hex byte gives 256 buckets per rule, so a 1e6-task rule puts ~4k
files per directory instead of 1e6 in one. Sharding under the rule
directory (not a remake2-style global shard pool) keeps per-rule `ls`
meaningful and costs small rules nothing in practice.

### Known limit: total file count

Sharding caps per-directory counts, not the total: a wide array job still
creates one log per task run, O(N_tasks) files (plus the scheduler's own
`.out`/`.err` pairs, which already have this property). On quota'd
filesystems this is a real budget at 1e5+ tasks. Not addressed now;
options if it bites, in escalating order:

1. a cleanup command / retention policy (e.g. `remake clean-logs`);
2. delete a task's log on success, keep on failure (loses successful-run
   logs — debugging value traded for inodes);
3. fold into the wider `.remake` layout rethink (discussion.md).

## Follow-ups (not in this change)

- `info --show-failures` could print each failed task's log path next to
  its stored traceback.
- The multiproc executor port should reuse the same per-task sink via
  `run-task`/worker entry points.
