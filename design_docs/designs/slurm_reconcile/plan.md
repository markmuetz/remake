# Plan: SLURM tasks killed without a result

**Status: Implemented, 2026-11-23 (0.11 item 9).** Replaces a larger plan
(sacct-based reconcile; see [implementation.md](implementation.md)).

## The problem

A SLURM array element killed by SLURM (node failure, time or memory limit)
dies without writing its result sidecar, so the DB never hears of it.

- **Wrong decisions** (rare): the task's older SUCCESS record stays. When
  nothing else marks the task stale, the next `run` takes it as up to
  date: a killed `run -f`, or a killed rerun under `check_outputs='always'`
  whose old outputs are still there. A killed *first* run under
  `check_outputs='fallback'/'always'` can have its partial outputs adopted.
  Other rerun reasons (code, `uses=`, io or paths changed, upstream reran)
  survive the kill, so those tasks rerun anyway (planner.py, the soak case).
- **Misreporting** (common, the soak's real harm): `slurm-status`, `info`,
  `why` and `task-info` gave no hint that SLURM had killed anything; the
  cause was found with `sacct` by hand.

## The design

1. **PENDING at submission.** After `submit.sh` runs (never on a dry run),
   the submitting process marks the tasks of every rule whose
   per-submission record (`jobs/<rule>.<run_seq>.jobids.json`) exists as
   PENDING: `metadata.mark_submitted(tasks, run_seq)`.
   - Only over records older than the submission (`run_seq <` it): a result
     this submission already delivered is kept.
   - The stored run_seq, code ids and resources are kept, as
     `set-state --pending` does: nothing new has run.
   - Tasks without a record get one, so `check_outputs` can't adopt what a
     killed worker left.
   - A plain UPDATE / INSERT OR IGNORE (no per-task hashing).
   - A finished element's sidecar (run_seq ≥) replaces it with SUCCESS or
     FAILED; a killed or never-started element stays PENDING and reruns
     (`last-run-pending`).
2. **Per-submission records** (`jobs/<rule>.<run_seq>.jobids.json`,
   written atomically with the submit time; `<jobid>_<index>` output
   names), so a later submission doesn't erase an earlier one's.
3. **Reporting, read-only** (nothing stored, so nothing to get wrong for
   good):
   - `slurm-status`: a `lost` column per rule's last submission: tasks
     neither queued (`squeue`) nor with a result. Not judged in the first
     minute (squeue may not list a new job yet).
   - `why` (for a pending or failed last run) and `task-info` (for a task
     without a success): one line from `task_slurm_state`: `queued (PD)` /
     `running (R)` from squeue; otherwise from one `sacct` call for that
     element, e.g. `NODE_FAIL on host1234 after 13:40`, or `no accounting
     record (cancelled before it started, or purged)`. A failed squeue or
     sacct is shown, never raised.
   - `info` is unchanged: it makes no SLURM calls, and submitted tasks
     already show as pending.

## Not done

- Recording the cause in the DB or history; a per-submission summary in
  `history --runs`. The cause is lost once sacct drops the job.
- Telling "killed" from "never started" without sacct.
- The deferred-rule continuation (`afterok` on every job,
  `--kill-on-invalid-dep=yes`) is cancelled by any failure in the
  submission; the deferred rules are planned by the next `remake run`.
  Documented; a smarter continuation needs its own small design (a failed
  task must not be resubmitted in a loop).
- `remake resubmit` reuses the run_seq, so a first attempt's results
  aren't marked pending again (it is to be retired at 1.0).
