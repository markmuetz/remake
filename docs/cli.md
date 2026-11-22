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
| `--profile NAME` | config profile (e.g. `jasmin`); beats `$REMAKE_PROFILE` and config files — see [Configuration](guide/configuration.md) |
| `--colour {auto,always,never}` | colourise output; `auto` (the default) colours TTYs only and honours `NO_COLOR`/`FORCE_COLOR` |

## Commands

| Command | Purpose |
|---|---|
| `run` | Run all pending (needed) tasks |
| `set-state` | Set tasks' recorded state by query, without running them |
| `verify` | Reconcile the DB with the outputs on disk (read-only unless `--apply`) |
| `info` | Per-rule summary of task statuses |
| `ls-tasks` | List tasks (key prefix + name), materialising matrices |
| `export-task` | Write a task's fully resolved spec as JSON |
| `rule-info` | Detail view of one rule: docstring, matrix, input/output templates, uses |
| `task-info` | Detail view of one task: status, paths, log, SLURM job |
| `task-log` | Print a task's per-task log |
| `why` | Explain why task(s) would (or would not) rerun |
| `lint` | Check input/output wiring between rules |
| `rule-dag` | Print the rule dependency DAG in topological order |
| `dag` | Export the rule or task graph as Graphviz DOT |
| `run-all` | Run every remakefile found in a directory |
| `slurm-status` | Live SLURM queue state of the last submission, per rule |
| `check-env` | Check a SLURM job would import the same code as this process (`--srun`, `--json`) |
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
| `--pending` | record pending: the tasks rerun at the next plan (the record, and its last measured resources, are kept) |
| `--check-outputs` | with `--success`, only tasks whose outputs are complete on disk |
| `--no-cascade` | with `--success`, stamp only the selected tasks (by default success also re-stamps downstream complete tasks so they aren't left looking stale; a guard skips any descendant with an independently-newer upstream) |
| `-n, --dry-run` | show affected tasks, change nothing |

## `verify`

`remake verify REMAKEFILE [-Q QUERY] [--outputs] [--adopt] [--apply] [--json]`
compares what the DB records with what is on disk. It only reports, unless
`--apply`. With neither `--outputs` nor `--adopt`, it does both.

| Option | Finds | With `--apply` |
|---|---|---|
| `--outputs` | tasks recorded as success whose outputs are missing, or fail their `Ensure` checks (e.g. after a scratch purge) | marks them pending (keeping their run stamp, so only they and what reads them rerun) |
| `--adopt` | tasks with no record whose outputs are all complete on disk (an existing output tree) | records them as success with the current code; a `Dir` without a manifest gets one; downstream settled tasks are re-stamped as by `set-state --success` |

Completion is checked in batches (one directory listing per directory, not
one `stat` per file). Rules whose matrix is not ready yet are not examined.
Exit code: 1 if anything was found and `--apply` was not given, else 0.

`verify --adopt --apply` is the recommended way to adopt existing outputs;
`set-state --success --check-outputs` still works, for when you know what
you are doing. remake never judges staleness by mtime (it is unreliable on
HPC filesystems); the DB is the record.

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
| `--failed` | one line per failed task (and nothing else): rule, short key, last line of its error, log path. The error is the stored exception's last line; if none is stored, the last non-empty line of the task's SLURM `.err` (`.remake/slurm/output/<rule>/<index>.err`). With `--json`, `{"format": 1, "failures": [{rule, key, error, log}, ...]}` |
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
  blocked / skipped) and exits like `run`: 1 if any failed, else 3 if any
  stopped short with rules left blocked, else 0. A remakefile that cannot be
  imported (e.g. a missing `xarray`) is reported as skipped.
- `-E/--executor`, `-j/--nproc`, `-f/--force`, `-n/--dry-run`,
  `--ignore-code-changes` and `--check-outputs` are passed to each `run`.

## `config`

`remake config pipeline.py [--json]` prints the effective merged config: the
selected profile, the config files consulted (found or not), and, per key,
the layer that set it. Does not touch the metadata database. See
[Configuration](guide/configuration.md).

## Inspecting rules and tasks

| Option | Meaning |
|---|---|
| `ls-tasks -i, --inputs` / `-o, --outputs` | show each task's input/output files, indented under it |
| `ls-tasks --check` | with `-i`/`-o`, stat each file and mark exists/complete (one stat per file — slow for large selections) |
| `rule-dag -N, --number-of-tasks` | annotate each rule with its task count as `rule[N]` (`?` when a dynamic matrix isn't resolvable yet) |
| `rule-dag -M, --matrix-keys` | annotate each rule with its matrix keys as `rule(m1, m2)` |
| `task-log --path` | print the log path only |
| `export-task <key>` / `-Q` | write the task's resolved spec (rule, key, kwargs, input/output paths, rule function module and qualname, remakefile path, `uses`) as JSON to stdout; `-o FILE` writes a file instead. `uses` values that aren't JSON-serialisable are given as their `repr`. See [Debugging](guide/debugging.md#running-a-task-body-from-another-python) |
| `ls-tasks --json` | the same per-task identity (`key`, `rule`, `kwargs`; plus `inputs`/`outputs` with `-i`/`-o`) for every task, under the `tasks` key of the [JSON output](#json-output) object |

## JSON output

Every `--json` output is one JSON object whose first key is `"format": 1`
(never a bare list). `format` is the version of these shapes. The top-level
keys per command:

| Command | Top-level keys after `format` |
|---|---|
| `info` | `rules`, `totals`; plus `tasks` with `-t`, `failures` with `-F` |
| `info --failed` | `failures` (list of `{rule, key, error, log}`) |
| `ls-tasks` | `tasks` (list of `{key, rule, kwargs}`, plus `inputs`/`outputs` with `-i`/`-o`) |
| `lint` | `findings` (list of rows, each with a `kind`) |
| `rule-dag` | `order`, `edges`; plus `rules` with `-N`/`-M` |
| `rule-info` | the rule's detail fields (`rule`, `docstring`, `matrix`, `outputs`, `uses`, ...) |
| `task-info` | the task's detail fields (`status`, `kwargs`, `inputs`, `outputs`, `slurm`, ...) |
| `config` | `profile`, `profile_source`, `files`, `config`, `provenance` |
| `slurm-status` | `submissions` (list of `{rule, jobid, states, reasons}`) |
| `check-env` | `planning`, `job`, `mismatches` |

Compatibility: after 1.0, adding a key is not a breaking change, so ignore
keys you do not know. Removing or renaming a key, or changing its type, is a
major-version change and bumps `format`. Before 1.0, shape changes are listed
under Breaking in the changelog. `export-task` prints a task spec (no
`format` key), and the JSONL event log (`.remake/remake.jsonl`) is not
`--json` output.

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
