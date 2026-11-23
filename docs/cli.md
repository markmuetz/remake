# CLI reference

Every command takes a remakefile path. Logs go to stderr; stdout carries
command output only, so `--json` output is clean for piping.

```bash
remake <command> pipeline.py [options]
```

## Global options

These go *before* the command name (`remake -D run pipeline.py`); `--colour`
may also go after it (`remake info --colour never pipeline.py`):

| Option | Meaning |
|---|---|
| `-T, --trace` / `-D, --debug` / `-I, --info` / `-W, --warning` | console log level (default: info) |
| `--profile NAME` | config profile (e.g. `jasmin`); beats `$REMAKE_PROFILE` and config files — see [Configuration](guide/configuration.md) |
| `--colour {auto,always,never}` | colourise output; `auto` (the default) colours TTYs only and honours `NO_COLOR`/`FORCE_COLOR` |

Tables (`info`, `slurm-status`, `history`) have a header row, numbers
right-aligned, and leave out a column with nothing in it. Output is fitted to
the terminal: a long name or path is shortened with `…` rather than wrapped.
The width is `$COLUMNS` if set (piped output too), else the terminal's; piped
output with no `$COLUMNS` is not shortened.

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
| `history` | A task's recorded executions, recent runs, or prune the history |
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

Deferred rules are not in `TOTAL`; lines under it count them and say why
each is deferred: `matrix not ready` (its own `@deferrable` matrix, or its
upstream reruns) or `held by <rule>` (it waits on that deferred rule; `run -n`
uses the same words). A deferred rule's `to run` is `?`. If a held rule's
matrix is static (a dict, a list or none), its tasks are known: it shows the
task count, and `failed` and `pending` from the DB, with `up-to-date` and
`stale` as `?` (they need the plan). A callable matrix there stays `?`.

In `--json` every deferred row has `deferred_by` (`[]` when its own matrix is
the cause); one with known tasks adds `tasks`, `succeeded`, `failed` and
`pending` (DB status, not crossed with the plan), and
`totals.deferred_tasks` sums them. `totals.failed` counts planned rules only:
failures in deferred rules show in their rows and in `--failed`.

| Option | Meaning |
|---|---|
| `-Q, --query` | filter tasks |
| `-t, --tasks` | list individual tasks with status |
| `-F, --show-failures` | show failures grouped by unique traceback signature + count |
| `--all-failures` | with `-F`, show every failed task individually (not grouped) |
| `--failed` | one line per failed task: rule, short key, last line of its error, and, when piped, the log path (in a terminal, a footer line says to use `remake task-log <key>` instead). The error is the stored exception's last line; if none is stored, the last non-empty line of the task's SLURM `.err` (`.remake/slurm/output/<rule>/<index>.err`). With `--json`, `{"format": 1, "failures": [{rule, key, error, log}, ...]}` |
| `--reasons` | per-rule tally of *why* the to-run tasks would rerun |
| `--json` | machine-readable output |

## `lint`

`remake lint REMAKEFILE` checks that every input of a rule is produced by one
of the rules it `depends_on`. Exit 1 if it finds a problem:

| Finding | Meaning |
|---|---|
| `NEAR MISS` | nothing produces the input, but a dependency produces a near-identical path (a likely typo) |
| `MISSING DEPENDENCY` | another rule produces the input, but it is not in `depends_on` |
| `DIR NESTED`, `FILE IN DIR` | an output inside another rule's `Dir` |
| `external` | no rule produces the input (information, not a problem) |

A near miss shows the two paths stacked, the shared leading directories
collapsed to `…`, and the difference in colour (red in the input, green in the
nearest output), or marked with `^` when colour is off (`--colour never`, or
not a terminal). Paths are compared directory by directory, so an extra
directory is one difference.

A rule whose matrix is deferred (not ready) has unknown outputs, but its output
templates are known: an input matching one (each `{field}` any text within one
path component) counts as that rule's, so a rule reading it without
`depends_on` is a missing dependency. A broad template (`data/{name}.nc`) can
claim an external file of the same shape; such a finding goes away once the
matrix is ready and the real outputs are known.
A callable outputs spec is called with a placeholder for each parameter it
names, to get the same templates. A deferred rule whose outputs still can't be
read (the call raises) is named in a note under the findings (`unchecked` on
the `near_miss` and `external` rows in `--json`).

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

## `history`

`remake history pipeline.py <key>` (or `-Q QUERY`) lists the recorded
executions of one task, newest first; `remake history pipeline.py --runs`
lists recent runs. Both read `remake.db` and run nothing. What is recorded,
and what it is for, is in [Provenance and run history](guide/provenance.md).

```bash
remake history pipeline.py 55a42548          # one task's executions
remake history pipeline.py -Q "year == 2015" # the query must match one task
remake history pipeline.py --runs --limit 5  # the last five runs
remake history pipeline.py --prune --keep 3  # trim the history now
```

| Option | Meaning |
|---|---|
| `<key>` / `-Q, --query` | the task, as for `task-info`: a key prefix, or a query matching exactly one task |
| `--runs` | list recent runs (one line each: run number, start time, command, tasks run and failed, short git commit with `+dirty`, and the run it continues) instead of one task's executions |
| `--limit N` | show at most N rows (default 20) |
| `--prune` | apply the per-task cap to every task now, and delete the per-task logs of tasks no longer in the pipeline (a changed matrix, a removed rule; 0.11) |
| `--keep N` | with `--prune`: keep N executions per task instead of `history.keep` (an error without `--prune`) |
| `--json` | machine-readable output |

An execution line shows the run number, finish time, status, resources, and
in brackets the rerun reason and host; a failed one is followed by its error
signature (`ExcType at file:line in function`), and a line says "ran in a
different environment from the planning run" for a SLURM job whose
environment differed. A task with no record prints "no recorded executions"
(history starts with 0.10; earlier runs are not back-filled).

The history keeps the last `history.keep` executions of each task (default
10), set in `remake.toml` (project file only):

```toml
[history]
keep = 20
```

The cap is applied as executions are recorded, so `--prune` is only needed
after lowering `keep` or to trim by hand. `--prune` prints
`pruned N history row(s)`. Runs themselves are never pruned.

`--json` shapes (see [JSON output](#json-output)): the task form is
`{"format": 1, "task", "key", "executions": [...]}`, each execution with
`run_seq`, `finished`, `status`, `wall_s`, `cpu_s`, `max_rss_bytes`,
`rss_method`, `output_bytes`, `reason`, `host`, `error` and
`different_environment`; `--runs` is `{"format": 1, "runs": [...]}` (the
provenance fields of the run, and its counts `n_planned`, `n_run`,
`n_succeeded`, `n_failed`, `n_skipped`, `n_blocked_rules`, `n_waves`);
`--prune` is `{"format": 1, "deleted": N}`.

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
| `lint` | `findings` (list of rows, each with a `kind`, `count` and `distinct`) |
| `rule-dag` | `order`, `edges`; plus `rules` with `-N`/`-M` |
| `rule-info` | the rule's detail fields (`rule`, `docstring`, `matrix`, `outputs`, `uses`, ...) |
| `task-info` | the task's detail fields (`status`, `kwargs`, `inputs`, `outputs`, `slurm`, ...); `slurm.state` as `why`'s `slurm`, for a task without a success |
| `history` | `executions` (with `task` and `key`); `runs` with `--runs`; `deleted` with `--prune` |
| `config` | `profile`, `profile_source`, `files`, `config`, `provenance` |
| `slurm-status` | `submissions`: one per rule's last submission, `{rule, jobid, states, reasons, submitted, tasks, succeeded, failed, lost}`; `submitted` is local time `YYYY-MM-DD HH:MM:SS`; `tasks` and the counts (results recorded by this submission) are `null` when unknown; `lost` (not queued, no result) is `null` in the first minute after submission |
| `why` | `tasks` (list of `{task, key, will_run, reasons: [{category, message}], slurm}`); `slurm` (a pending or failed last run in a SLURM submission, else `null`): `{jobid, index, run_seq, element, summary}` |
| `set-state` | `state`, `dry_run`, `skipped`, `tasks` (list of `{task, key, cascade}`) |
| `verify` | see [`verify`](#verify) |
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

`-Q` takes a small restricted expression evaluated over each task's matrix
kwargs (and `rule`, the rule name):

```bash
remake info pipeline.py -Q "rule == 'process' and year >= 2015"
remake run  pipeline.py -Q "rule in ['extract', 'process']"
```

`-Q True` matches every task.

The grammar is deliberately small: names (matrix kwargs and `rule`), literals
(numbers, strings, `True`/`False`/`None`, lists and tuples of them),
comparisons (`==  !=  <  <=  >  >=  in  not in  is  is not`), `and` / `or` /
`not`, parentheses and a unary minus (`year > -5`). Anything else (function
calls such as `len(x)` or `range(...)`, attributes, subscripts, arithmetic,
sets, dicts, comprehensions, lambdas, f-strings) is rejected before the query
is evaluated, with exit code 2 and a message naming the construct. A name that
is no rule's matrix key (and not `rule`) is an error too, which catches typos.

A query that names a kwarg a rule doesn't have simply doesn't match that
rule's tasks — it is not an error. `-Q "site == 'oxford' and year < 2015"`
selects no tasks from an upstream rule whose matrix has only `site`. To include
such a rule in a subset, select it by name:

```bash
remake run pipeline.py -Q "site == 'oxford' and (rule == 'setup' or year < 2015)"
```

Preview with `remake info -Q ...` (or `run -n -Q ...`) to see what matched.
