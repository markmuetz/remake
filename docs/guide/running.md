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

`-I/--ignore-code-changes` reruns only tasks that have never *succeeded*
(e.g. previously failed), ignoring code-hash changes. Upstream propagation
still applies so fan-ins pick up newly-run elements.

```bash
remake run pipeline.py -I
```

## Exit codes

`run` reports outcome through its exit code, so it composes in scripts and CI:

| Code | Meaning |
|---|---|
| `0` | success — everything needed completed |
| non-zero | one or more tasks failed (see [Debugging](debugging.md)) |

## Forcing state

`set-state` records task state without running, for adopting an existing tree
of outputs or resetting:

```bash
# mark matching tasks succeeded, verifying outputs exist first
remake set-state pipeline.py -Q True --success --check-outputs

# mark matching tasks pending (will rerun next time)
remake set-state pipeline.py -Q "rule == 'process'" --pending
```

`--success --check-outputs` is the adoption path: lock in a pipeline whose
outputs already exist without recomputing them.
