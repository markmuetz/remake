# Testing SLURM reconciliation

**Status: Agreed in outline by MM (2026-11-23); under advisor review.**
Companion to [plan.md](plan.md).

Reconciliation is about rare, timing-dependent events (a node dies, an
accounting DB lags, a sidecar lands late, a user reruns mid-flight). The
existing SLURM tests (`tests/integration/test_slurm.py`) use canned
`sbatch`/`squeue` shims: good for generation and submission, but they
can't run jobs, kill them, or report accounting. This work gets its own
test section.

## Layout

```
tests/slurm/
  __init__.py
  conftest.py            fixtures: a project dir with the fake cluster on PATH
  fake_cluster.py        the simulated SLURM cluster (below)
  test_fake_cluster.py   the simulator's own tests (dependencies, arrays, sacct output)
  test_sacct_parse.py    sacct output parsing, incl. real JASMIN samples
  test_scenarios.py      one named test per case (table below)
  test_state_machine.py  Hypothesis stateful tests: random event schedules
  live/
    test_live_slurm.py   the same scenarios on a real cluster (JASMIN)
    README.md            how to run them
```

Naming follows the repo: `tests/<kind>/test_*.py`; a pytest marker
`live_slurm` (registered in pyproject.toml) marks the live tests, which
are skipped unless `--live-slurm` is given (a conftest option) and
`sbatch` is on PATH. `uv run pytest` runs everything else, fast.

## The simulated cluster (`fake_cluster.py`)

A small in-process model of the parts of SLURM remake relies on, exposed
as executables on PATH (`sbatch`, `squeue`, `sacct`, `scancel`) that read
and write one JSON state file, plus a Python controller the tests drive.

Model:
- Jobs and array elements, each with a state (PENDING, RUNNING,
  COMPLETED, FAILED, CANCELLED, NODE_FAIL, TIMEOUT, OUT_OF_MEMORY,
  PREEMPTED, REQUEUED…), start/end times, node, exit code.
- `sbatch --parsable` parses the `#SBATCH` lines remake writes (array
  range and throttle, `--dependency=afterok/aftercorr/afterany:…`,
  `--kill-on-invalid-dep`, output paths) and returns a job id.
- Dependencies: an element becomes eligible when its dependency is met;
  when a dependency can never be met and `kill-on-invalid-dep=yes`, it is
  CANCELLED without starting (DependencyNeverSatisfied), as SLURM does.
- `squeue` and `sacct` print in the exact formats remake requests,
  including collapsed pending ranges (`1001_[3-7%2]`) and `CANCELLED by
  <uid>`. Faults: either can be made to fail (exit 1, timeout); sacct can
  lag (an element that ended isn't in accounting yet, or still shows
  RUNNING).
- A logical clock; no sleeping.

Controller (what tests call):
- `start(job, index, node=…)`, `finish(job, index)`: **runs the real
  `remake run-array-task`** for that element (in-process, in the project
  dir), so sidecars, logs and outputs are real. Exit code from it.
- `kill(job, index, state)`: NODE_FAIL / TIMEOUT / OUT_OF_MEMORY /
  PREEMPTED / CANCELLED; the worker never runs, or runs partway
  (`partial=True` writes some output then stops, no sidecar).
- `requeue(job, index)`, `cancel_job(job)`, `fail_sacct()`,
  `lag_sacct(n_ticks)`, `fail_squeue()`, `late_sidecar(...)` (hold a
  worker's sidecar back and release it later).
- `drain()`: run everything eligible to completion (the happy path).

The cluster runs remake's real submission path: `remake run -E slurm`
writes `submit.sh`, which calls the fake `sbatch`. Continuation /
end-of-submission jobs are jobs like any other; `finish` runs their
script's `remake` command.

## Scenario tests (`test_scenarios.py`)

One test per case, each asserting what reconcile classifies, what it
writes, and what `slurm-status`, `info`, `why`, `task-info`, `history
--runs` and the next `run -n` show:

| Scenario | Expectation |
|---|---|
| all succeed | all done; reconciled; no SLURM calls afterwards |
| NODE_FAIL mid-array (the soak case) | killed (NODE_FAIL) recorded failed; dependents not_started; next run reruns both |
| TIMEOUT, OUT_OF_MEMORY, PREEMPTED | killed with that reason |
| CANCELLED by a failed upstream | not_started; no record; old record kept |
| scancel of a running element | killed (CANCELLED) |
| FAILED before the task ran (bad import) | killed, `SlurmFailed` |
| COMPLETED but no sidecar | result_missing, nothing written |
| requeued after NODE_FAIL, then succeeds | never killed; done |
| sacct fails | unknown; nothing written; command doesn't crash |
| squeue fails | unknown; nothing written |
| sacct lag | unknown until accounting catches up, then classified |
| late sidecar after a killed record | the real result wins |
| run-82 case: resubmission before reconcile | the earlier submission is still reconciled from its own files |
| killed `--force` rerun of a successful task | next plan reruns it (was: up to date) |
| killed first run + `check_outputs='fallback'` | partial output not adopted |
| deferred rules + an unrelated failure (phase 2) | end-of-submission job runs; continuation submitted without the failed chain |
| deterministic failure in a chain (phase 2) | not resubmitted in a loop |
| end-of-submission job itself lost (phase 2) | next `run`/`slurm-status` reconciles |
| legacy sidecars (pre-0.9, pre-0.11 `%a.out`) | still read; no crash |

## Hypothesis state-machine tests (`test_state_machine.py`)

A `RuleBasedStateMachine` over a few fixed pipelines (a chain, a fan-out
with `aftercorr`, one with a deferred rule). Rules generate the events:
submit (`remake run -E slurm`), start/finish/kill/requeue an eligible
element, cancel a job, make squeue/sacct fail or lag, hold/release a
sidecar, run a read-only command, run reconcile. Invariants checked after
every step:

1. No element is `killed`/`not_started` unless the simulator says it
   ended without its worker writing a result.
2. A worker's real result is never lost or overwritten by a killed record.
3. A failed squeue/sacct call leads to no DB writes.
4. Reconcile is idempotent: a second call changes nothing.
5. Once the cluster is drained and accounting has caught up, every
   submission is reconciled with no `unknown`.
6. The next plan never calls a killed task up to date.
7. No task is submitted twice while an element for it is active.
8. (phase 2) The continuation runs whenever deferred rules exist and the
   end-of-submission job ran; it never resubmits a task that failed in
   its chain.

Each step is cheap (in-process remake commands on a tiny project), so a
few hundred steps per run is affordable; `settings(max_examples=…,
stateful_step_count=…)` keeps CI time to a minute or so, with a larger
profile (`HYPOTHESIS_PROFILE=deep`) for local runs. Failing examples are
shrunk to a minimal event sequence and added to `test_scenarios.py`.

## Live tests (`live/test_live_slurm.py`, JASMIN)

The scenarios that can be produced on a real cluster, to check the
simulator's model against SLURM itself:

- TIMEOUT (a task that sleeps past `--time`), OUT_OF_MEMORY (allocates
  past `--mem`), CANCELLED (scancel while running), dependency-cancelled
  (a failing upstream), FAILED before the task (a broken import),
  requeue (`scontrol requeue`), sacct immediately after a job ends.
- NODE_FAIL can't be caused on demand; it is covered by the simulator,
  with formats checked against the soak's archived sacct output
  (`/work/scratch-nopw2/mmuetz/remake_soak/run81_node_fail/`), copied as
  fixtures into `test_sacct_parse.py`.
- Each live test records the raw `squeue`/`sacct` output it saw into a
  fixtures dir, so format surprises become offline tests.

Run on JASMIN (account and setup: see the soak notes):
`uv run pytest tests/slurm/live --live-slurm` from a scratch checkout.
