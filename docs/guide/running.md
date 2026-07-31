# Running pipelines

```bash
remake run pipeline.py
```

`run` plans the DAG, works out which tasks are needed, and executes them with
the chosen executor.

remake operates from the remakefile's own directory: `remake run sub/pipeline.py`
changes into `sub/` first, so the `.remake/` store, the log, and the pipeline's
relative input/output paths are all anchored to the remakefile rather than to
wherever you launched the command. Running from the remakefile's directory (the
usual case) is unaffected.

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
| non-zero | one or more tasks failed (see [Debugging](debugging.md)) |

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
- Tasks run **concurrently inside one process** cannot be told apart —
  memory and CPU are per-process facts. remake detects this and records only
  wall time. It affects a dask run pointed at an external cluster whose
  workers use multiple threads; remake's own local cluster, `multiproc` and
  SLURM all give each task its own process.
- A task killed by the OOM killer or by SLURM's time limit records
  **nothing** — it never gets to report. Its state stays as if it never ran;
  use `sacct` for those.
- The numbers describe the **last execution**, not the current state:
  `set-state` does not clear them.
- Where remake cannot measure memory reliably it records nothing rather than
  a wrong number, and annotates any non-sampled figure — `peak rss 1.4G
  (rusage)` is a whole-process reading, which includes the interpreter.

Sampling is on by default and costs a background thread per running task.
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
