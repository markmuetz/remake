# CLI reference

Every command takes a remakefile path. Logs go to stderr; stdout carries
command output only, so `--json` output is clean for piping.

```bash
remake <command> pipeline.py [options]
```

## Commands

| Command | Purpose |
|---|---|
| `run` | Run all pending (needed) tasks |
| `set-state` | Set tasks' recorded state by query, without running them |
| `info` | Per-rule summary of task statuses |
| `ls-tasks` | List tasks (key prefix + name), materialising matrices |
| `task-info` | Detail view of one task: status, paths, log, SLURM job |
| `task-log` | Print a task's per-task log |
| `why` | Explain why a task would (or would not) rerun |
| `lint` | Check input/output wiring between rules |
| `rule-dag` | Print the rule dependency DAG in topological order |
| `slurm-status` | Live SLURM queue state of the last submission, per rule |
| `resubmit` | Re-execute `.remake/submit.sh` without replanning |
| `version` | Print remake version |

`run-task` and `run-array-task` also exist but are invoked by remake itself
(the latter by generated SLURM scripts); you rarely call them directly.

## `run`

```bash
remake run pipeline.py [options]
```

| Option | Meaning |
|---|---|
| `-E, --executor` | `singleproc` (default), `multiproc`, `slurm`, or `module:Class` |
| `-j, --nproc` | worker processes for `multiproc` (default: all cores) |
| `-Q, --query` | filter tasks by a kwargs query |
| `-f, --force` | force rerun of matched tasks |
| `-I, --ignore-code-changes` | run only tasks that have never succeeded |
| `-n, --dry-run` | show what would run, run nothing |
| `--check-outputs` | verify outputs of completed tasks (always mode) |
| `-X, --debug-exception` | force `singleproc` and drop into pdb/ipdb on first failure |
| `--raise` | force `singleproc` and re-raise the first failure (no debugger) |

## `set-state`

| Option | Meaning |
|---|---|
| `-Q, --query` | tasks to affect (**required**; `-Q True` for all) |
| `--success` | record success with current code/uses hashes |
| `--pending` | delete records — tasks become never-run |
| `--check-outputs` | with `--success`, only tasks whose outputs are complete on disk |
| `-n, --dry-run` | show affected tasks, change nothing |

## `info`

| Option | Meaning |
|---|---|
| `-Q, --query` | filter tasks |
| `-t, --tasks` | list individual tasks with status |
| `-F, --show-failures` | show stored tracebacks of failed tasks |
| `--json` | machine-readable output |

## Selecting a single task

`task-info`, `task-log` and `why` take either a task key (a prefix is enough)
or `-Q` to select by kwargs query:

```bash
remake why pipeline.py <key-prefix>
remake task-log pipeline.py -Q "site == 'oxford' and year == 2015"
```

## Queries

`-Q` takes a Python expression evaluated over each task's matrix kwargs:

```bash
remake info pipeline.py -Q "rule == 'process' and year >= 2015"
remake run  pipeline.py -Q "rule in ['extract', 'process']"
```

`-Q True` matches every task.
