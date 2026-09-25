# remake reference (for the tutor)

What remake does and what its commands and output mean, condensed from
the user docs. Answer learner questions from this. The words in
**bold** match what remake prints. A test (tests/integration/test_tutorial.py)
checks this file names every CLI command, `info` column, `why` reason and
`check_outputs` mode.

## Core ideas

- **remakefile**: an ordinary Python file (here `pipeline.py`) defining
  rules and registering them on a `Remake()` object, usually with
  `rmk.rules_from_current_module()`.
- **rule**: a function decorated with `@rule(...)`. It declares
  `inputs=` and `outputs=` (dicts of name → path template), an optional
  `matrix=`, `depends_on=` (upstream rules), `uses=` (values and helpers
  the code relies on) and `config=`.
- **task**: one unit of work. A rule without a matrix has one task; with a
  matrix it has one task per combination of matrix values. A task is
  identified by its **rule plus its matrix values**, shown as
  `clean[site=exeter, year=2023]` (`clean[]` with no matrix).
- **task key**: a stable ID (a sha1 of rule name + matrix values), shown by
  its first 8 hex digits, e.g. `a7b8c0bc`. Any unique prefix works where a
  command takes a key.
- **Signature contract**: `def fn([inputs,] [outputs,] <matrix keys>)`,
  in that order. The matrix keys must match the function's other
  parameters, and remake checks this when it loads the file.
- **matrix forms**: a dict of lists (cartesian product), such as
  `{'site': [...], 'year': [...]}`; a list of dicts (explicit combinations);
  a tuple key binding several values together; or a callable returning a
  list of dicts. `@deferrable` marks a callable that may wait on upstream
  outputs. Matrix values must be plain scalars (str, int, float, bool,
  None).
- **Templates**: `{site}` and `{year}` in paths are filled in per task. An
  inputs spec can also be a function of the matrix values
  (`inputs=lambda year: {...}`).
- **depends_on** links rules. Which upstream *tasks* a task depends on comes
  from its declared inputs: the upstream tasks whose outputs it reads. A
  task with no path link to an upstream rule, or one reading a file that
  several upstream tasks write, depends on all of that rule's tasks.
  **Declare what you read.**
- **uses=**: module-level values and helper functions a rule's code
  relies on. They are tracked, so a change to one reruns the rule. Globals
  the code reads that aren't in `uses=` produce a scope warning, and
  changes to them are **not** tracked.

## What makes a task run

remake runs a task when (these are the `why` reason categories):

| reason | meaning |
|---|---|
| **never-run** | no record in the DB |
| **last-run-failed** / **last-run-pending** | its last run failed, or is recorded as unfinished |
| **code-changed** | the rule function's code changed (compared as an AST: comments and formatting don't count) |
| **uses-changed** | a `uses=` value or helper changed |
| **io-changed** | the rule's inputs/outputs spec changed |
| **outputs-missing** | outputs are missing and outputs are being checked (`--check-outputs`, or `check_outputs='always'`) |
| **upstream-rerun** | an upstream task it reads from reruns this run |
| **upstream-newer** | an upstream task it reads from ran in a later invocation than it did, so its output may be stale |
| **adopted-outputs** | (not a rerun) no record, but its outputs are complete on disk and `check_outputs` adopts them |

remake **never** looks at file modification times. By default
(`check_outputs='never'`) it doesn't check files exist either: the DB is
the truth.

## `.remake/` (next to the remakefile)

- `remake.db`: SQLite, the record of every task (status, code/uses/io
  fingerprints, a run counter, timings, memory);
- `remake.log`: a human-readable log of commands (INFO and up);
- `remake.debug.log`: the same, in full detail;
- `remake.jsonl`: the same as structured JSON, one record per line (the
  tutor's watcher reads this);
- `run.lock`: present only while a `remake run` is running;
- `tasks/log/`: per-task logs, read with `task-log`. They're written only
  when tasks run in worker processes (`-E multiproc`, dask, SLURM). A plain
  `remake run` (singleproc) logs to `remake.log` only, so `task-log` has
  nothing for its tasks and says so.
- `slurm/` and `jobs/`: SLURM scripts and submission records, when SLURM
  is used.

## `remake info` columns

A four-way split of each rule's tasks: **up-to-date + stale + failed +
pending = tasks**, and **up-to-date + to run = tasks**.

| column | meaning |
|---|---|
| **tasks** | how many tasks the rule has (its matrix size) |
| **up-to-date** | succeeded, and `run` would not run it again |
| **stale** | succeeded last time, but `run` would rerun it (code, `uses=` or io changed, or an upstream reruns) |
| **failed** | its last run failed (it will rerun) |
| **pending** | never run, or a run is in flight |
| **to run** | what `remake run` would run now = stale + failed + pending |

Stored task statuses are just **success**, **failed** and **pending**;
*up-to-date* and *stale* come from combining the record with what the plan
would do. Options:
- `-t/--tasks` lists each task with its status;
- `-F/--show-failures` groups failures by traceback, and `--all-failures`
  lists each one;
- `--reasons` tallies why the to-run tasks would run;
- `--json` gives machine-readable output.

## Commands

Global options go **before** the command: `-T/--trace`, `-D/--debug`,
`-I/--info`, `-W/--warning` (console log level);
`--colour {auto,always,never}`.

| command | what it does |
|---|---|
| `run` | run the tasks that need running |
| `info` | per-rule summary of task states (above) |
| `ls-tasks` | list tasks (key + name); `-i`/`-o` show input/output files, `--check` marks whether they exist |
| `why` | explain why task(s) would or wouldn't run (all reasons, with code diffs); takes a key, `-Q`, or nothing (the whole to-run set) |
| `task-info` | one task in detail: status, paths, log, resources (wall, CPU, peak memory) |
| `task-log` | print one task's log (`--path`: just the path) |
| `rule-info` | one rule in detail: docstring, matrix, templates, uses |
| `rule-dag` | the rule dependency graph in order (`-N` task counts, `-M` matrix keys) |
| `lint` | check input/output wiring between rules (near-miss paths, missing depends_on) |
| `set-state` | record task state without running (below) |
| `slurm-status` | live SLURM queue state of the last submission |
| `resubmit` | re-run `.remake/submit.sh` without replanning (SLURM) |
| `version` | print the version |
| `run-task`, `run-array-task` | internal: run one task (used by executors and SLURM scripts) |

`run` options:

| option | meaning |
|---|---|
| `-Q/--query` | only consider the tasks the query matches |
| `-f/--force` | run the matched tasks even if up to date |
| `-n/--dry-run` | show the plan, run nothing |
| `--check-outputs` | check the outputs of finished tasks; rerun the missing ones (and adopt complete outputs with no record) |
| `--ignore-code-changes` | run only tasks that never succeeded; skip code/uses/io checks (upstream reruns still apply) |
| `-E/--executor` | `singleproc` (default), `multiproc`, `slurm`, or `module:Class` |
| `-j/--nproc` | worker processes for multiproc |
| `-X/--debug-exception` | run in-process and drop into the debugger on the first failure |
| `--raise` | run in-process and re-raise the first failure |

`task-info`, `task-log` and `why` select by a task key (prefix) or `-Q`.
`task-info` and `task-log` need exactly one match.

## Queries (`-Q`)

A Python expression over each task's matrix values plus `rule` (the rule
name), for example:
- `"year >= 2022"`
- `"site in ['aberdeen', 'exeter']"`
- `"rule == 'clean' and year == 2020"`

`-Q True` matches everything. A name that no rule has is an error (a typo
check). A task whose rule lacks a name the query uses simply doesn't
match. A few builtins are allowed (`range`, `len`, `min`, `max`, `abs`,
`any`, `all`, …).

## `check_outputs` modes (`Remake(check_outputs=...)`)

| mode | behaviour |
|---|---|
| **never** (default) | the DB only; a task with no record runs even if its output exists |
| **fallback** | a task with no record counts as done if its outputs are on disk (for adopting an existing tree) |
| **always** | also check the outputs of recorded tasks, and rerun if any are missing |

`run --check-outputs` is "always" for one run. `never` is the default
because under `fallback`, clearing a task's record after editing its code
would silently re-adopt the old output.

## set-state

Records state without running anything:
- `--success` stamps the tasks with the current code;
- `--pending` deletes their records, so they will run;
- `-Q` is required.

`--success --check-outputs` stamps only the tasks whose outputs exist: the
way to adopt existing outputs into a fresh `.remake/`. `--success`
cascades to already-finished downstream tasks by default, so they don't
look stale; turn that off with `--no-cascade`. `-n` shows what would
change.

## Failures

A failing task's traceback is recorded, and tasks that read its outputs are
**skipped** for that run. The run goes on with everything else. See them
with `info -F` (grouped tracebacks), `task-info`, or `task-log` (worker
executors only). Fix the code and run again: the
failed tasks (and anything skipped) run, and the successes don't.

## Exit codes (`run` and other commands)

| code | meaning |
|---|---|
| `0` | success |
| `1` | a task failed, or a rule was left blocked (a deferred matrix never became ready) |
| `2` | usage error: bad arguments, bad query, broken remakefile, bad rule dependencies (`error: ...`) |
| `130` / `143` | interrupted by Ctrl-C / SIGTERM (running tasks stopped, workers cleaned up) |

## Running

- Only one `remake run` at a time per directory (`.remake/run.lock`). A lock
  left by a crash on the same machine is cleared automatically.
- `multiproc` runs tasks in parallel in local worker processes. `slurm`
  submits a job array per rule. A worker killed by the OOM killer only
  fails its own task.
- Output directories are created for you.
- remake runs from the remakefile's own directory, so relative paths and
  `.remake/` sit next to it.
