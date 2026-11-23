# Testing SLURM reconciliation

**Status: Agreed by MM (2026-11-23); revised after the Fable advisor's
review the same day.**
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

A small model of the parts of SLURM remake relies on. The executables on
PATH are **dumb bash shims**, like the existing ones (a Python start-up per
call would make the state-machine tests far too slow): `sbatch` appends its
arguments to a log, **copies the script** (SLURM does; the next submission
rewrites `slurm/<rule>.sbatch`) and prints the next job id; `squeue` and
`sacct` `cat` files that the controller re-renders after every state
change; each can be switched to fail (exit 1) or hang. All parsing and
modelling happens in the test process.

Model (built from the soak archive and the step-0 live sample, not from
memory):
- Jobs and array elements, each with a state, start/end on a logical
  clock, node, exit code; and how many times squeue and sacct were called.
- Submission: the controller reads the sbatch log and the copied scripts
  (array range and throttle, `--dependency=afterok/aftercorr/afterany`,
  `--kill-on-invalid-dep`, output paths). A dependency on a job id the
  controller has forgotten is **rejected**, as real sbatch does, so the
  `submit.sh`-aborts-under-`set -e` path is testable.
- Dependencies: an element becomes eligible when its dependency is met;
  when it can never be met and `kill-on-invalid-dep=yes`, it is CANCELLED
  without starting, and, as in the archive, may leave no sacct row of its
  own (one record for the never-split remainder).
- `squeue -r` output: **one element per line** (no collapsed ranges);
  finished jobs stay listed in their terminal state for a few ticks
  (MinJobAge). `sacct` output: the requested fields, collapsed pending /
  never-split ranges (`1001_[3-7%2]`), `CANCELLED by <uid>`, Start
  `None`/`Unknown`, `None assigned`; can lag (an ended element missing or
  still RUNNING) or be purged (no rows for an old job).
- `%A_%a.out/.err` files are written when an element starts.

Controller (what tests call):
- `start(job, index, node=…)`, `finish(job, index)`: **runs the real
  `remake run-array-task`** in-process (as `tests/integration/test_slurm.py`
  already does), with `SLURM_ARRAY_JOB_ID`/`SLURM_ARRAY_TASK_ID` and the
  script's `export` lines set for the call and **restored** afterwards (a
  leaked `REMAKE_PARENT_RUN_SEQ` changes later submissions).
- `kill(job, index, state, partial=False)`: NODE_FAIL / TIMEOUT /
  OUT_OF_MEMORY / PREEMPTED / CANCELLED / BOOT_FAIL. `partial=True` runs
  the task with a function that writes some output and then raises a
  `BaseException` subclass (not caught by `run_task`), so no sidecar is
  written; to check the CLI wrapper lets it through.
- `requeue`, `cancel(job, index=None)` (pending or running), `purge(job)`
  (sacct forgets it), `fail_sacct()`, `lag_sacct(n)`, `fail_squeue()`,
  `hold_sidecars()`/`release_sidecars()`, `tick(n)`, `drain()` (run
  everything eligible to completion).
- The end-of-submission job is a job like any other; `finish` runs its
  `remake` command.

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
| an ordinary task failure (sidecar FAILED) | failed by the worker; dependents not_started; no killed record |
| user scancel of a pending element; of a whole array | not_started; no records |
| BOOT_FAIL; an unrecognised state | not_started or killed by the started rule; unknown |
| requeued, ran partly, then cancelled while pending | killed (the `.out` file shows it started) |
| run 81 reconciled after run 82 succeeded | superseded; nothing written |
| `set-state` between the kill and the reconcile | superseded |
| `resubmit` (same run_seq, new job id) | the new job is reconciled; the old one isn't confused with it |
| `submit.sh` aborted midway (sbatch rejects a dependency) | submitted rules reconciled; the rest never recorded as submitted |
| rule removed or renamed; spec pruned; sacct purged | expired; settles; no SLURM calls afterwards |
| upgrade: legacy pointer files, in-flight pre-upgrade arrays (`%a.out`), a queued pre-upgrade `afterok` continuation | old pointers settled as legacy unless queued or recent; no reruns caused by the upgrade |
| two reconciles of the same killed element | one task record, one history row |
| aftercorr dependents of killed elements with no sacct rows (archive) | not_started |
| `metadata_dir` relocated | files and table found there |
| a run whose every rule was skipped as already queued | no end-of-submission job |
| a `-Q` run with deferred rules (phase 2) | the continuation keeps the query |
| reconcile seconds after submission (squeue not listing the job yet) | unknown, not not_started |
| element killed seconds after starting; sacct not split out, `.out` not visible | unknown until the start grace passes or evidence arrives; then killed |
| one sacct chunk fails, the others succeed | that chunk's elements unknown |
| stale `<index>.out` / reused job id's old `<jobid>_<index>.out` | not start evidence |

## Hypothesis state-machine tests (`test_state_machine.py`)

A `RuleBasedStateMachine` over a few fixed pipelines (a chain, a fan-out
with `aftercorr`, one with a deferred rule). The machine owns its temp
dir, cwd and PATH (state machines can't use function-scoped fixtures like
`tmp_path`/`monkeypatch`). Rules generate the events: submit (`remake run
-E slurm`), start/finish/kill/requeue/cancel an eligible element, make
squeue/sacct fail or lag or purge, hold/release sidecars, tick the clock,
`set-state`, run a read-only command, run reconcile. Local `run` is left
out (invariant 7 would fail by design). Invariants, checked after every
step:

1. An element is `killed`/`not_started` only if the simulator says it
   ended without its worker writing a result.
2. For every result a simulated worker wrote, the task record has a
   run_seq ≥ its own, and, if equal, the worker's status.
3. A step in which squeue or sacct failed writes no killed records and
   sets no `reconciled` flags (ingest and the submission import still
   write).
4. With cluster state and clock unchanged, a second reconcile changes
   nothing.
5. Once faults are cleared, held sidecars released, accounting caught up
   and the grace period passed, every submission is reconciled with no
   `unknown`.
6. After a successful reconcile, a task whose latest ended element was
   killed, with no later worker result, is in the plan.
7. No task key is in two active SLURM submissions at once.
8. (phase 2) No task key appears in two submissions of one chain; and
   after a clean drain, every task not downstream of a failed, killed or
   cancelled one is SUCCESS.
9. Per submission, the outcome counts sum to `n_tasks`.
10. Once everything is settled, commands make zero squeue/sacct calls.
11. Read-only commands allocate no run_seq and add no `run` row.

Each step is an in-process remake command on a tiny project, and the
shims are bash `cat`s, so a few hundred steps per run fit in CI with
`settings(max_examples=…, stateful_step_count=…)` kept to about a minute;
`HYPOTHESIS_PROFILE=deep` runs longer locally. A failing example is
shrunk to a minimal event sequence and added to `test_scenarios.py`.

## Live tests (`live/test_live_slurm.py`, JASMIN)

The scenarios that can be produced on a real cluster, to check the
simulator's model against SLURM itself:

- First (plan step 0): one sacct sample covering a completed, a failed,
  a timed-out, a scancelled-running and a cancelled-pending element and an
  aftercorr dependent, plus `SLURM_TIME_FORMAT=%s`, `-S`, and sacct on a
  compute node. Saved as fixtures.
- TIMEOUT (a task that sleeps past `--time`), OUT_OF_MEMORY (allocates
  past `--mem`), CANCELLED (scancel while running), dependency-cancelled
  (a failing upstream), FAILED before the task (a broken import),
  requeue (`scontrol requeue`), sacct immediately after a job ends.
- Requeue is disabled on JASMIN (`scontrol requeue`: "presently
  disabled"), so the requeue scenarios rest on the simulator alone.
- NODE_FAIL can't be caused on demand; it is covered by the simulator,
  with formats checked against the soak's archived sacct output
  (`/work/scratch-nopw2/mmuetz/remake_soak/run81_node_fail/`), copied as
  fixtures into `test_sacct_parse.py`.
- Each live test records the raw `squeue`/`sacct` output it saw into a
  fixtures dir, so format surprises become offline tests.

Run on JASMIN (account and setup: see the soak notes):
`uv run pytest tests/slurm/live --live-slurm` from a scratch checkout.
