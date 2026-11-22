# Provenance and run history

From 0.10 remake records, in `.remake/remake.db`, what each run ran with and
what happened to each task each time it ran. It answers "which commit and
environment produced this output?", "why did this task rerun last Tuesday?"
and "did it fail the same way before?" without you having to keep anything.
Nothing needs switching on, and upgrading reruns nothing. Runs from before
0.10 have no history ("not recorded").

**None of it is a rerun trigger.** Like checksums and the recorded config,
provenance is written down, never compared. Changing your git commit,
installing a package, upgrading Python or editing the lockfile does not make
a task rerun; only the things in [Running pipelines](running.md) do (code,
`uses`, input and output specs, upstream reruns). A change shows up in
`task-info` and `history`, nothing more.

## What a run records

Each invocation that writes to the metadata (`remake run`, and the
commands that change recorded state) adds one row to the `run` table, with
the command name; a SLURM submission's `run-task` children link back to it
(`parent_run_seq`, shown as "continues" in `history --runs`):

| Field | Meaning |
|---|---|
| git commit, dirty flag, diff hash | the commit of the repository holding the remakefile; whether tracked files differ from it; and, when they do, a sha256 of `git diff HEAD`, so two dirty runs can be told apart. An untracked remakefile counts as dirty. Nothing is recorded outside a repository or without `git` |
| Python and remake versions | the interpreter and remake that ran |
| package fingerprint | a sha256 over the Python version and the sorted `name==version` list of every installed distribution (read with `importlib.metadata`, no subprocess). The full list is stored once per distinct fingerprint, not per run |
| lockfile and its hash | the nearest `uv.lock`, `pixi.lock`, `conda-lock.yml` or `poetry.lock`: beside the remakefile, or, inside a git repository, in a parent up to the repository root |
| host, hashed username | the hostname, and the first 16 hex digits of the sha256 of the login name |
| config | the effective config (as before 0.10) |
| counts | tasks planned, run, succeeded, failed, skipped, rules left blocked, and waves |

`remake history pipeline.py --runs` lists them, one line per run; add
`--json` for every field.

## What an execution records

Each time a task runs (or fails) one row is added to the `task_run` table:

| Field | Meaning |
|---|---|
| run, finish time, status | which run executed it, when, and whether it succeeded |
| resources | wall and CPU time, peak memory (sampled, so Linux only), as in `task-info` |
| output bytes | total size of the task's outputs |
| rerun reason | why the planner chose to run it (below) |
| host | where it ran: for SLURM, the compute node |
| error signature | for a failure, `ExcType at file:line in function` of the deepest frame, without the message, so tasks failing the same way share one |

The last `history.keep` executions per task are kept (default 10; see
[`history`](../cli.md#history)). `remake history pipeline.py <key>` lists
them, newest first. `remake task-info` shows the last one, with the git
commit and environment of the run that executed it.

```
$ remake history pipeline.py 55a42548
55a42548 calibrate16[]  55a4254836b25c98ddc9cc9ff99e7c5366d9d801
run     3  2026-11-22 15:38:28  success  wall 0.00s, cpu 0.00s  [last-run-failed, host]
run     2  2026-11-22 15:38:21  failed   wall 0.00s, cpu 0.00s  [forced, host]
      RuntimeError at ex16_diagnostics.py:110 in calibrate16
run     1  2026-11-22 15:38:15  success  wall 0.00s, cpu 0.00s  [never-run, host]
```

### Rerun reason categories

The category is the planner's first-listed reason for running the task, the
same names `remake info --reasons` tallies (`remake why` gives the full
messages):

| Category | The task ran because |
|---|---|
| `never-run` | it had no record |
| `forced` | `-f/--force` |
| `last-run-failed`, `last-run-pending` | its last run failed, or was left pending (`set-state --pending`) |
| `code-changed`, `uses-changed`, `io-changed` | its rule's code, its `uses`, or its input/output specs changed. Several joined with `+` if more than one did, e.g. `code-changed+io-changed` |
| `paths-changed` | a callable input/output spec now resolves to other paths |
| `outputs-missing` | `check_outputs='always'` found an output missing or failing its checks |
| `upstream-rerun` | an upstream task reran in this pass |
| `upstream-newer` | an upstream task was recorded by a later run than this one |

(`adopted-outputs` and `up-to-date` are also planner categories, but a task
in either state does not run, so they never appear on an execution.)

## SLURM

A SLURM array job runs in a new process, possibly in a different environment
from the one that planned and submitted it. The job computes the same
fingerprint and writes it into its sidecar **only if it differs from the
planner's**, with the package list in `.remake/environments/<fingerprint>.json`
(once per environment, not per task). Ingest records it against the tasks it
ran. `task-info` and `history` then say "ran in a different environment from
the one that planned it"; `remake check-env` is the tool for finding out
what differs *before* submitting. When the environments match nothing extra
is stored and no flag appears. The execution's host is the compute node.

## Privacy

`.remake/` is a record of who ran what and where, so think before sharing or
archiving it.

- The **username is hashed** (sha256, truncated), not stored. The hash is
  *pseudonymous, not anonymous*: it is unsalted, so anyone with a list of
  candidate usernames can compute theirs and match. It lets you tell that two
  runs were by the same person, not who that was to someone who does not
  already know.
- The **diff itself is never stored**, only its hash, because diffs can be
  large and can contain secrets. The commit hash and the hash cannot be
  turned back into your changes.
- **Hostnames are stored in plain text**, in runs and in executions. An
  archived `.remake/` (in a data deposit, an RO-Crate, a bug report) contains
  the names of the machines and clusters you used. The package list names every
  installed distribution, including private ones.
- Error signatures hold a file name, a line and a function name, never the
  exception message.

Delete `.remake/` (after archiving what you need) or `remake history --prune
--keep 1` to cut the per-task history down; the run rows stay.

## Reading it yourself

It is ordinary SQLite: tables `run`, `task_run`, `environment` (the package
lists, by fingerprint) in `.remake/remake.db`. Prefer `remake history --json`
(a stable, versioned shape, see [JSON output](../cli.md#json-output)) to
reading the tables, whose layout may change between minor versions. No
command yet compares two environments' package lists.
