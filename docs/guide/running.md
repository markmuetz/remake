# Running pipelines

```bash
remake run pipeline.py
```

`run` plans the DAG, works out which tasks are needed, and executes them with
the chosen executor.

remake operates from the remakefile's own directory: `remake run sub/pipeline.py`
changes into `sub/` first, so the `.remake/` store, the log, and the pipeline's
relative input/output paths are all anchored to the remakefile rather than to
wherever you launched the command (`metadata_dir` in `remake.toml` can move
`.remake/` elsewhere; see [Configuration](configuration.md)). Running from the remakefile's directory (the
usual case) is unaffected.

The same holds when you use remake as a library (since 0.10). A `Remake(...)`
built in a script anchors at that script's directory, so `python
path/to/pipeline.py` from anywhere writes `data/` and `.remake/` beside the
script. Pass `Remake(..., project_dir=...)` to choose another directory. With
no defining file (a notebook cell, the REPL, `python -c`) it anchors at the
current directory when it is created. `rmk.project_dir` says where. The
defining file is the first caller outside remake: a helper that builds the
`Remake` in a shared package anchors at that package, so pass
`project_dir=` there. Each
`Remake` method changes into that directory for its duration and changes
back afterwards. Since the working directory is process-wide, don't drive two
`Remake`s from different threads at once. Paths on tasks you hold after a
method returns stay relative to `project_dir`: open them from there, or
join them to `rmk.project_dir`. Likewise create a `Sqlite3Backend` with an
explicit path if you pass `metadata=`; its default location is relative to
the current directory at construction.

## Executors

Select with `-E/--executor`:

| Executor | Use |
|---|---|
| `singleproc` | one process — simplest, best for debugging |
| `multiproc` | local parallelism; spawned workers reload the remakefile |
| `slurm` | submit to a SLURM cluster (see [SLURM](slurm.md)) |

```bash
remake run pipeline.py -E multiproc -j 8
```

`-j/--nproc` sets the worker count for `multiproc`.

## Running a subset

Use a query (`-Q`) to restrict which tasks are considered:

```bash
remake run pipeline.py -Q "rule in ['extract', 'process']"
```

## Only rerun what never succeeded

`--ignore-code-changes` reruns only tasks that have never *succeeded*
(e.g. previously failed), ignoring code-hash changes. Upstream propagation
still applies so fan-ins pick up newly-run elements.

```bash
remake run pipeline.py --ignore-code-changes
```

## Exit codes

`run` reports outcome through its exit code, so it composes in scripts and CI:

| Code | Meaning |
|---|---|
| `0` | success — everything needed completed |
| `1` | one or more tasks failed (see [Debugging](debugging.md)) |
| `2` | usage error — bad arguments, missing external inputs, an invalid or unknown-name `-Q` query, a missing or broken remakefile, bad rule dependencies — printed as `error: ...` |
| `3` | the run stopped short: no task failed, but rules were left blocked — a deferred matrix that never became ready; each is logged with what it is waiting on. Rerunning may finish the job once the missing upstream exists. (Before 0.10 this was `1`.) |
| `130` / `143` | interrupted by Ctrl-C / SIGTERM (128 + the signal number) |

If tasks failed *and* rules were left blocked, the code is `1`.

## Missing external inputs

Before running anything, `remake run` checks the inputs of the tasks about
to run. If any are missing **and no rule produces them** (the `external`
class of `remake lint`), it stops with one error instead of one
`FileNotFoundError` per task, listing them grouped by rule (a few examples
each, then "... and N more", with a total count) and exits `2`. Inputs that
another rule makes are never checked, even if absent: they will exist by the
time they are needed. Plain paths are checked with `os.path.exists`, custom
tokens with `is_complete()`; each distinct path is stat'ed once, and only for
tasks that are actually going to run. The check runs on the first planning
wave only, and for the SLURM executor it runs at submission time on the
login node, before any job is submitted.

With `-n/--dry-run` the problem is reported as a warning but the plan is
still shown and the exit code is unaffected.

To add a pipeline-specific nudge to the error, set a hint in the remakefile:

```python
rmk = Remake(config={'external_inputs_hint': 'Run download.py first.'})
```

## One run at a time

`remake run` holds a lock (`.remake/run.lock`) while it runs, so a second
`run` started in the same directory stops with an error instead of running
the same tasks again and racing on their outputs. The lock is released when
the run ends, including on Ctrl-C; one left by a crashed run on the same
machine is detected and replaced automatically. If the error names another
host and no run is active there, delete `.remake/run.lock`.

## Checking outputs on disk

By default the **metadata DB is the sole source of truth** for what has run.
A task is considered done only if the DB says so — the mere presence of an
output file on disk does *not* count. This is the `check_outputs='never'`
mode (the default), set on the `Remake()` object:

```python
rmk = Remake(check_outputs='never')   # the default
```

The three modes:

| Mode | Behaviour |
|---|---|
| `'never'` (default) | DB only. A task with no DB record reruns even if its output exists. |
| `'fallback'` | A task with no DB record is treated as done if its outputs are complete on disk. The **migration** mode — adopt a pre-existing tree into a fresh `.remake/`. |
| `'always'` | Every planned task's outputs are checked, catching outputs deleted behind the DB's back (e.g. a scratch purge). |

Why `'never'` is the default: under `'fallback'`, editing a task's code and
then clearing its record with `set-state --pending` would *silently re-adopt*
the old output instead of rerunning — the edit is swallowed. With `'never'`,
clearing the record always forces a rerun.

`--check-outputs` on `run` turns on `'always'` mode for that invocation
(and also adopts complete outputs with no DB record, like `'fallback'`):

```bash
remake run pipeline.py --check-outputs    # verify/recover outputs this run
```

Tasks with no declared outputs are always DB-authoritative — there is nothing
to check.

## Resource use per task

Every task execution records how long it took and how much memory it used,
for all executors. `task-info` shows the last execution's numbers:

```
resources: wall 12.40s, cpu 11.98s, peak rss 1.4G
```

- **wall** — elapsed time; **cpu** — user+sys CPU of the task process and any
  children it waited for. `cpu` well under `wall × cores` on a multi-core
  allocation means you asked for cores the task never used.
- **peak rss** — the high-water resident memory of the task process and of
  any child processes it ran. Use it to size a SLURM `mem` request.

Some caveats worth knowing before you act on the numbers:

- Peak memory is **sampled** (every 100 ms by default), so an allocation
  spike shorter than the interval can be missed. Resident memory also counts
  shared pages, so shared libraries inflate small tasks a little.
- The figure is the memory the *process* held while the task ran, so it has
  a floor: where a worker process runs several tasks in turn, memory an
  earlier task left resident (Python does not always hand freed memory back
  to the OS) counts towards the next task's peak. It is the right number for
  "how much do I request for this task", but a cheap task following an
  expensive one in the same worker can look dearer than it is.
- Memory used by **child processes** (a task that shells out to `cdo`, `ncks`
  and the like) is measured as the *largest single child* the process has
  reaped, not the sum of children running at once, and the figure is the
  **max** of that and the task process's own peak, not their sum. Three
  300 MiB children in parallel read as roughly 300 MiB. The operating system
  also only reports a child high-water mark that never decreases, so in a
  worker that already ran a bigger child, a later task's smaller children
  cannot be seen. When that happens remake records the method as
  `sample-self` (or `rusage-self`): the figure covers the task process only
  and the child peak was **not measured**. Treat it as a lower bound.
- Tasks run **concurrently inside one process** cannot be told apart —
  memory and CPU are per-process facts. remake detects this and records only
  wall time. It affects a dask run pointed at an external cluster whose
  workers use multiple threads; remake's own local cluster, `multiproc` and
  SLURM all give each task its own process.
- A task killed by the OOM killer or by SLURM's time limit records
  **nothing** — it never gets to report. Its state stays as if it never ran;
  use `sacct` for those.
- The numbers describe the **last execution**, not the current state:
  `set-state` (including `--pending`) does not clear them.
- Where remake cannot measure memory reliably it records nothing rather than
  a wrong number, and annotates any non-sampled figure — `peak rss 1.4G
  (rusage)` is a whole-process reading, which includes the interpreter.

Sampling is on by default and costs a background thread per running task.
That thread makes the process multi-threaded, so on Python 3.12+ a task that
calls `os.fork()` directly gets a `DeprecationWarning` (an error under
`-W error`); `capture: False` avoids it.
Turn it off (keeping the free wall/CPU timings) with:

```python
rmk = Remake(config={'resources': {'capture': False}})
```

`rss_interval` sets the sampling period in seconds:
`config={'resources': {'rss_interval': 0.01}}`.

## Forcing state

`set-state` records task state without running, for adopting an existing tree
of outputs or resetting:

```bash
# mark matching tasks succeeded, verifying outputs exist first
remake set-state pipeline.py -Q True --success --check-outputs

# mark matching tasks pending (will rerun next time)
remake set-state pipeline.py -Q "rule == 'process'" --pending
```

`--success --check-outputs` is the explicit adoption path: lock in a pipeline
whose outputs already exist without recomputing them. Because the default
`check_outputs='never'` mode never adopts on-disk outputs implicitly, this is
how you migrate an existing output tree into remake.

`set-state --success` also **cascades** by default: it re-stamps the matched
tasks *and* their already-complete downstream tasks, so settling a mid-pipeline
task doesn't leave its descendants looking stale and needlessly rerunning (a
guard skips any descendant that has a genuinely-newer other upstream). Use
`--no-cascade` to stamp only the matched tasks.
