# Running pipelines

```bash
remake run pipeline.py
```

`run` plans the DAG, works out which tasks are needed, and executes them with
the chosen executor.

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
