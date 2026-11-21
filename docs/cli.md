# CLI reference

Every command takes a remakefile path. Logs go to stderr; stdout carries
command output only, so `--json` output is clean for piping.

```bash
remake <command> pipeline.py [options]
```

## Global options

These go *before* the command name (`remake -D run pipeline.py`):

| Option | Meaning |
|---|---|
| `-T, --trace` / `-D, --debug` / `-I, --info` / `-W, --warning` | console log level (default: info) |
| `--colour {auto,always,never}` | colourise output; `auto` (the default) colours TTYs only and honours `NO_COLOR`/`FORCE_COLOR` |

## Commands

| Command | Purpose |
|---|---|
| `run` | Run all pending (needed) tasks |
| `set-state` | Set tasks' recorded state by query, without running them |
| `info` | Per-rule summary of task statuses |
| `ls-tasks` | List tasks (key prefix + name), materialising matrices |
| `rule-info` | Detail view of one rule: docstring, matrix, input/output templates, uses |
| `task-info` | Detail view of one task: status, paths, log, SLURM job |
| `task-log` | Print a task's per-task log |
| `why` | Explain why task(s) would (or would not) rerun |
| `lint` | Check input/output wiring between rules |
| `rule-dag` | Print the rule dependency DAG in topological order |
| `dag` | Export the rule or task graph as Graphviz DOT |
| `run-all` | Run every remakefile found in a directory |
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
| `--ignore-code-changes` | run only tasks that have never succeeded |
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
| `--no-cascade` | with `--success`, stamp only the selected tasks (by default success also re-stamps downstream complete tasks so they aren't left looking stale; a guard skips any descendant with an independently-newer upstream) |
| `-n, --dry-run` | show affected tasks, change nothing |

## `info`

The per-rule table is a four-state partition of each rule's tasks — the counts
always satisfy `up-to-date + stale + failed + pending = tasks` and
`up-to-date + to run = tasks`:

| Column | Meaning |
|---|---|
| `up-to-date` | succeeded and the planner would not rerun it |
| `stale` | succeeded, but would rerun (code/`uses=`/io changed, or an upstream reruns) |
| `failed` | last run failed (will rerun) |
| `pending` | never run, or a run is in flight |
| `to run` | what `remake run` would do now = `stale + failed + pending` |

| Option | Meaning |
|---|---|
| `-Q, --query` | filter tasks |
| `-t, --tasks` | list individual tasks with status |
| `-F, --show-failures` | show failures grouped by unique traceback signature + count |
| `--all-failures` | with `-F`, show every failed task individually (not grouped) |
| `--reasons` | per-rule tally of *why* the to-run tasks would rerun |
| `--json` | machine-readable output |

## `dag`

`remake dag [--rules | --tasks] [-o FILE] REMAKEFILE` writes the graph as
Graphviz DOT text (stdout by default; no Graphviz install needed to produce
it, e.g. `remake dag p.py | dot -Tsvg -o dag.svg`). Nothing is run and no
metadata is read.

| Option | Meaning |
|---|---|
| `--rules` | the rule-level graph (the default) |
| `--tasks` | one node per task, with edges derived from the tasks' declared input/output paths; a task that depends on every task of an upstream rule gets an edge from each; a rule whose matrix is deferred appears as one node labelled `(deferred)` |
| `-o, --output FILE` | write to FILE instead of stdout |

`--tasks` expands every matrix, so it is only sensible for modest pipelines.

## `run-all`

`remake run-all [DIR]` (default: the current directory) finds the remakefiles
in DIR and runs each as if by `remake run <file>`, in sorted-filename order.

- Discovery is an AST scan of `DIR/*.py` for a `Remake(...)` call; files are
  never imported to decide (so `make_example_data.py` is not run). `-r`
  also searches subdirectories (needed for `examples/ex5_multifile/pipeline.py`).
- Remakefiles are treated as independent: **no ordering promises** beyond the
  sort order, and no dependencies between them. Generate any input data first
  (e.g. `python make_example_data.py`), then `remake run-all`.
- It continues past a failing remakefile, prints a summary (ran / failed /
  skipped) and exits non-zero if any failed. A remakefile that cannot be
  imported (e.g. a missing `xarray`) is reported as skipped.
- `-E/--executor`, `-j/--nproc`, `-f/--force`, `-n/--dry-run`,
  `--ignore-code-changes` and `--check-outputs` are passed to each `run`.

## Inspecting rules and tasks

| Option | Meaning |
|---|---|
| `ls-tasks -i, --inputs` / `-o, --outputs` | show each task's input/output files, indented under it |
| `ls-tasks --check` | with `-i`/`-o`, stat each file and mark exists/complete (one stat per file — slow for large selections) |
| `rule-dag -N, --number-of-tasks` | annotate each rule with its task count as `rule[N]` (`?` when a dynamic matrix isn't resolvable yet) |
| `rule-dag -M, --matrix-keys` | annotate each rule with its matrix keys as `rule(m1, m2)` |
| `task-log --path` | print the log path only |

## Selecting tasks for `task-info`, `task-log` and `why`

All three take either a task key (a prefix is enough) or `-Q` to select by
kwargs query:

```bash
remake why pipeline.py <key-prefix>
remake task-log pipeline.py -Q "site == 'oxford' and year == 2015"
```

`task-info` and `task-log` need the selection to match exactly one task.
`why` explains *every* match, and with no key and no query it explains the
whole runnable set.

## Queries

`-Q` takes a Python expression evaluated over each task's matrix kwargs:

```bash
remake info pipeline.py -Q "rule == 'process' and year >= 2015"
remake run  pipeline.py -Q "rule in ['extract', 'process']"
```

`-Q True` matches every task.

A query that names a kwarg a rule doesn't have simply doesn't match that
rule's tasks — it is not an error. `-Q "site == 'oxford' and year < 2015"`
selects no tasks from an upstream rule whose matrix has only `site`. To include
such a rule in a subset, select it by name:

```bash
remake run pipeline.py -Q "site == 'oxford' and (rule == 'setup' or year < 2015)"
```

Preview with `remake info -Q ...` (or `run -n -Q ...`) to see what matched.
