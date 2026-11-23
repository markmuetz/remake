# Testing

**Status: Implemented, 2026-11-23.** Companion to [plan.md](plan.md).

## Layout

```
tests/slurm/
  conftest.py            the `cluster` fixture: a project dir with the simulator on PATH;
                         --live-slurm option and the `live_slurm` marker
  fake_cluster.py        the simulated SLURM cluster
  test_fake_cluster.py   the simulator's own tests, incl. a replay of the live probe
  test_sacct_parse.py    sacct parsing, against the soak archive and the live probe
  test_records.py        per-submission records, output names, pruning
  test_scenarios.py      kills and what remake does and shows afterwards
  fixtures/              real sacct/squeue output (anonymised)
```

## The simulated cluster

Dumb bash shims for `sbatch`, `squeue`, `sacct` and `scancel` (they log
calls and print files), driven by a Python controller on a logical clock.
`finish()` runs the real `remake run-array-task` in-process; `kill(state,
partial=)` ends an element without a result (`partial`: after it wrote its
outputs); dependencies, `kill-on-invalid-dep`, `scancel`, faults (`fail`,
`forget`, `purge`) and lags are modelled. `sacct` output follows what JASMIN
printed (tests/slurm/fixtures/jasmin_probe_2026-11-23/); the probe's job
sequence replays through the shims to the same rows.

## Scenarios (`test_scenarios.py`)

all succeed; a killed forced rerun is rerun (with its dependents); a killed
first run's partial output is not adopted; a result already ingested isn't
marked pending; a dry run marks nothing; only rules actually submitted are
marked (submit.sh stopped at a rejected dependency); `slurm-status` counts
lost tasks; `why`/`task-info` show the sacct cause, or the queue state
without calling sacct; a failed sacct is shown, not raised.

## Live tests

The live probe on JASMIN (step 0) is kept as fixtures. Requeue is disabled
on JASMIN, so it isn't modelled beyond the simulator.
