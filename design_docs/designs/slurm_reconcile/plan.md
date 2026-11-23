# Plan: reconciling SLURM submissions

**Status: Agreed by MM (2026-11-23); revised after the Fable advisor's
review the same day (outcomes made total, chain mode defined, write
semantics, gate liveness, upgrade path).** Background: soak finding 7
(design_docs/records/soak_0_11.md) and the advisor's design review of the
same date. Decisions 1–8 are in [README.md](README.md).

## 1. The problem

remake learns a SLURM task's outcome only from the result sidecar the
worker writes when the task ends (`run-array-task` → `SidecarWriter`,
ingested later by whichever process next plans or reports). A worker that
is killed (NODE_FAIL, TIMEOUT, OUT_OF_MEMORY, PREEMPTED, scancel) or never
starts (CANCELLED by a failed dependency, BOOT_FAIL) writes nothing. Today:

- Every command misreports it. `slurm-status` says "not queued", `info`
  counts it as stale (mixed with tasks in flight), `why` gives a true but
  secondary reason, `task-info` shows the job and index but not its fate.
- It can be a correctness bug. A killed `--force` rerun leaves the old
  SUCCESS record, so the next plan calls the task up to date, possibly over
  a half-written output; with `check_outputs = 'fallback'/'always'`, a
  killed first run's partial file can be adopted.
- The deferred-rule continuation is submitted with `afterok` on every job
  and `--kill-on-invalid-dep=yes`: any failure anywhere cancels it, and
  deferred rules are never planned, silently.
- The evidence is overwritten. `.remake/jobs/<rule>.jobids.json` is one
  file per rule; a resubmission replaces it, and `slurm/output/<rule>/%a.out`
  is reused by every submission (an index may map to a different task).

The fix records the facts as they happen, reconciles each submission once
all its jobs have ended, and records the outcome in the DB so later
commands don't need SLURM at all.

## 2. Concepts

- **Submission**: one rule's array job from one `remake run -E slurm`,
  identified by `(rule, run_seq)`: a job id, the immutable spec file
  `jobs/<rule>.<run_seq>.json` (array index → task key), and a submit time.
  A rule skipped as already queued is not a new submission.
- **Element**: one array index of a submission = one task.
- **Element outcome** (what reconcile decides, per element). The rules
  are state-agnostic, so every SLURM state lands somewhere; a state on
  neither list below is `unknown`.
  - ACTIVE states (squeue): PENDING, RUNNING, REQUEUED, REQUEUE_HOLD,
    REQUEUE_FED, RESIZING, SUSPENDED, COMPLETING, CONFIGURING, STAGE_OUT,
    SIGNALING (what `INACTIVE_STATES` doesn't exclude today).
  - TERMINAL states (sacct): COMPLETED, FAILED, CANCELLED, NODE_FAIL,
    TIMEOUT, OUT_OF_MEMORY, PREEMPTED, BOOT_FAIL, DEADLINE, REVOKED.
  - **Started** = the element's **own** sacct row has a Start timestamp
    and a NodeList other than `None assigned`, **or** its `%A_%a.out`
    exists (written when the batch script starts; closes the
    requeued-then-cancelled hole). Rows with Start `None`/`Unknown` are
    never evidence of starting, and never evidence about one element
    alone: sacct shows an array's unsplit pending elements as one row,
    labelled with a range (`J_[0,2-3]`) or, for aftercorr arrays, with a
    single index (`J_5` stood for elements 0, 2, 3, 5 in the live probe).

| Outcome | Evidence | Written to the DB |
|---|---|---|
| `done` | a task record with run_seq **equal** to the submission's | nothing new |
| `superseded` | a record with run_seq **greater** (a later run or `set-state` got there first) | nothing |
| `queued` / `running` | squeue lists the element in an ACTIVE state | nothing |
| `killed` | TERMINAL, not COMPLETED, started; no record ≥ run_seq | FAILED record + history row, reason `Slurm<State>: …` |
| `not_started` | not listed by squeue (`-r` lists every pending element individually), and not started; whatever sacct shows (often **no row at all**: aftercorr-cancelled and individually cancelled pending elements vanish from sacct, live probe and soak archive) | nothing (counted as skipped) |
| `result_missing` | COMPLETED, no record, after a grace period | nothing; reported (quota, quarantined sidecar) |
| `expired` | not in squeue, no sacct row at all for the job, and older than the spec prune age (7 days); or its spec file / rule is gone | nothing; settles the submission |
| `unknown` | squeue or sacct failed; or started but no terminal sacct state yet (accounting lag); or an unlisted state; or COMPLETED inside the grace period | nothing; reported |

  The not_started rule comes from the soak archive (after 6133148_11/12/15
  died, the aftercorr dependents 6133150_11/12/15 have no sacct rows; only
  the remainder row 6133150_16 appears) and was confirmed by the live
  probe (`tests/slurm/fixtures/jasmin_probe_2026-11-23/`). It needs no
  sacct row: squeue says the element is gone, and nothing shows it ran.

- **Settled / reconciled**: a submission is reconciled when every element
  is `done`, `superseded`, `killed`, `not_started`, `result_missing` (past
  grace) or `expired`. Reconciled submissions are never queried again.

Rule: **unknown is never lost.** Nothing is written for `unknown`.

## 3. Records (on-disk contract additions)

1. **Per-submission job-id files.** `submit.sh` writes
   `jobs/<rule>.<run_seq>.jobids.json` (`{"v": 1, "slurm_array_job_id",
   "run_seq", "submitted"}`) via a temp file and `mv` (a shell redirect can
   leave a truncated file), and keeps `jobs/<rule>.jobids.json` as the
   "latest" pointer (same content, same atomic write). The files **own**
   the job id and submit time.
2. **The end-of-submission job's id**: `jobs/<run_seq>.finish.json` (a name
   no rule can have: `_finish` is a valid rule name and the
   `*.jobids.json` globs treat the prefix as one).
3. **Job output names include the job id**: `%A_%a.out/.err`. Readers
   (`_failed_line`, `task-log`) use the task's submission's job id and fall
   back to `%a.*` for pre-upgrade submissions. The simulator writes them.
4. **Pruning** (step 2, not later): `prune_spec_files` also prunes
   per-submission jobids files (today it skips every `*.jobids.json`) and
   the `%A_%a` outputs of pruned submissions, by the same 7-day rule.
5. **DB: a `slurm_submission` table** (schema 1.2, additive; created by the
   idempotent `_schema_statements`; a 1.1 remake opens a 1.2 DB and ignores
   it). Keyed by **rule name** and run_seq (a removed or renamed rule's
   submission can still settle): `(rule, run_seq, jobid, n_tasks,
   submitted, reconciled TIMESTAMP NULL, counts JSON)`. The table **owns**
   `reconciled` and `counts`. `counts` is a snapshot at reconcile time
   (informational; keys may be added); `slurm-status` takes done/failed
   live from task records. Import from the files is an upsert on (rule,
   run_seq) that updates the job id and clears `reconciled` when it
   changed (`resubmit` reuses the run_seq with a new job id).
6. **Killed-task records**: a new metadata method (not `_ingest_records`
   as is):
   - one SQL upsert with a **strict** guard in the statement itself:
     `WHERE task.run_seq IS NULL OR excluded.run_seq > task.run_seq`
     (another process may ingest the real sidecar at any moment; a later
     real result, at run_seq ≥, still overwrites the killed record through
     the normal ingest guard);
   - on conflict it updates only status, exception, run_seq and timestamp:
     code ids, `paths_digest` and resources stay as the last real run left
     them (nothing ran to completion; `why` can still diff against it);
   - the history row is written **only if the upsert applied**
     (`rowcount`), with `finished` = sacct End normalised to the DB's UTC
     `YYYY-MM-DD HH:MM:SS` (sacct runs with `SLURM_TIME_FORMAT` set, so no
     local-time/DST ambiguity; the unique index then makes repeats one
     row), `host` = NodeList, `wall_s` = Elapsed, `reason` = the spec's
     per-task reason, `error` through `error_signature` as usual, the full
     text in `task.exception`: `SlurmNodeFail: job 6133148_11 on host1234
     after 13:40 (run 81)`.
   - A metadata backend without these methods (third-party) makes
     reconcile a no-op with a debug log.
7. Optionally, workers add `slurm_job` (`<array job id>_<index>`) to their
   result sidecar, recorded in `task_run` (nice to have).

## 4. `sacct_snapshot`

Beside `squeue_snapshot`:

```
SLURM_TIME_FORMAT=%s sacct -X -n -P -S <earliest submit> -j <id1>,<id2>,… \
      --format=JobID,State,ExitCode,Start,End,Elapsed,NodeList
```

- `SLURM_TIME_FORMAT=%s` (epoch seconds) if JASMIN's sacct accepts it,
  else an explicit ISO format; `-S` from the earliest submit time unless
  the live check shows `-j` alone searches all time. `-X`: no steps.
- Returns `{base_id: {index: Element(state, start, end, elapsed, node,
  exit)}, …}` plus, per job, whether it has any rows at all.
- Parses collapsed elements (`6133152_[0]`, `6133151_[0-33]`,
  `_[0-3,7%4]`); `CANCELLED by 12345` → `CANCELLED`; Start `None`/`Unknown`
  → no start.
- Raises `SacctError` on a non-zero exit, a timeout, or output it can't
  parse. Batched, chunked for long id lists.
- The format and behaviour are taken from the soak archive
  (`tests/slurm/fixtures/sacct_run81_node_fail.psv`) and a live sample
  taken **before** the simulator is written (step 0).

## 5. `reconcile`

One idempotent function `reconcile(rmk, *, run_seq=None, write=True,
now=None, squeue_timeout=None)` in `core/reconcile.py`:

1. Ingest sidecars (as today).
2. Import submissions from the per-submission files into the table (§3.5).
   **Legacy pointer files** (pre-0.11, no per-submission file): imported
   only if the job is in squeue or the file is younger than the prune age;
   otherwise settled at once as `legacy` (never written as killed: an
   upgrade must not cause reruns).
3. Select unreconciled submissions (all, or those at `run_seq`). None:
   return at once, with no SLURM calls (the gate).
4. Snapshot squeue (with a short timeout for read-only callers; 60 s
   only for `run`/the end job), **then** ingest again, then read records:
   a result that lands in between is `done`, never `killed`. squeue
   rows in a terminal state (MinJobAge keeps them ~5 min) count as not
   in the queue.
5. One sacct call for elements not done and not active.
6. Classify (§2), with `now` as the clock (grace period, expiry).
7. Write killed records (§3.6) if `write`.
8. Update counts; set `reconciled` when settled. Stamp the run's counts
   for runs whose submissions are all reconciled (`n_failed` includes
   killed; `n_skipped` includes not_started).
9. Return a report per submission and element for the commands.

Reconcile **never allocates a run_seq** (`current_run_seq()` allocates)
and adds no `run` row.

## 6. Callers

- **`run` (any executor, and `run -n`)** reconciles before planning, so a
  killed task has a FAILED record and is rerun. Off-cluster with
  unreconciled submissions: one warning, plan with what the DB says.
  Reconcile knows which planned tasks are queued or running, so a local
  `run` warns about them (nearly free; the rest of "local vs SLURM" stays
  deferred).
- **The end-of-submission job** (phase 2) reconciles its own run_seq.
- **`slurm-status`, `info`, `why`, `task-info`**: reconcile first, gated;
  `SqueueError`/`SacctError` → a warning and `unknown`, not a crash
  (`slurm-status` crashes on `SqueueError` today).
- `n_planned` counts what the executor actually submitted (today it
  includes rules skipped as already queued).

## 7. Reporting

- `slurm-status`: per rule's latest submission, columns `tasks, PD, R,
  done, failed, killed, not started, reason`; a killed breakdown by state
  (`NODE_FAIL 4`) in the reason column or under the table. `--json` gains
  `killed` (by state), `not_started`, `result_missing`, `superseded`,
  `expired`, `unknown`, and the end job's state.
- `why`: when the task's last submission killed it, that comes first:
  `last attempt killed: job 6133148_11 NODE_FAIL on host1234 after 13:40
  (run 81)`; other reasons follow as secondary (dashed, nested).
- `task-info`: `slurm:` line gains the fate (`killed: NODE_FAIL on
  host1234 after 13:40`), from the history row.
- `info`: no new column (killed tasks are failed, decision 1); a one-line
  hint under the table when any failure is a SLURM kill (`4 failed tasks
  were killed by SLURM (NODE_FAIL); see remake slurm-status`), and
  `unknown` elements are mentioned.
- `history --runs`: the run's tasks cell gains `4 killed (NODE_FAIL),
  3 not started`.
- `info --failed`: the error column shows the `Slurm…` reason.

## 8. Phase 2: the end-of-submission job

Replaces the continuation job.

- Submitted when this run submitted at least one array (or deferred rules
  remain and there are queued refs to wait on); never by a run that
  submitted nothing (or it could spawn another, forever).
  `--dependency=afterany` on all the submission's refs (`afterany` never
  becomes invalid, so `kill-on-invalid-dep` never kills it). Resources as
  the continuation today (10 min, 1G).
- Command: `remake slurm-finish <remakefile> --run-seq N --chain P
  [--continue] [-Q <query>]`. Its arguments are a **compatibility
  contract**: SLURM copies the script at submission, so a job queued by
  one version runs under the next (compatibility.md says so).
  `--continue` is decided at submit time (deferred rules exist), so the end
  job doesn't plan unconditionally and submit whatever the user made stale
  meanwhile. `-Q` carries the original run's query (today's continuation
  drops it: a filtered run continues over the whole pipeline; fixed here).
  `REMAKE_PARENT_RUN_SEQ` stays readable for pre-upgrade continuation jobs
  only (sbatch exports the environment, so the variable leaks into every
  job a continuation submits).
- It runs:
  1. `reconcile(run_seq=N)`, retrying for up to ~2 min while sacct shows
     non-terminal states (slurmdbd lag); the rest stays unreconciled for
     the next caller.
  2. With `--continue`: plan, then **chain mode**: the continuation never
     submits a task already submitted in this chain. Excluded: every task
     key in the chain's spec files that is not SUCCESS at run_seq ≥ the
     chain root, plus everything downstream (walked in the plan's
     topological order with the local executors' `record_failure` /
     `upstream_failed` helpers, seeded from the spec keys via
     `task_from_spec`, since a killed `--force` task with no record is not
     in the runnable list). This covers failed, killed, unknown (sacct
     still lagging), result_missing and user-cancelled tasks alike.
     Chain members are runs whose `run.parent_run_seq` is P (plus P
     itself), not "run_seq ≥ P": a manual `run` during the chain is not
     part of it. Logged: `excluded: N tasks submitted earlier in this
     chain and not successful; rerun with remake run`. Deferred rules
     downstream of a failure stay deferred by themselves (the planner
     defers below a rerunning upstream).
  3. Logs a one-line submission summary.
- If it is lost, cancelled or never runs, the next `run`/`slurm-status`
  reconciles (the backstop); only the continuation is lost, as today, but
  now visibly (`slurm-status`: `finish job 1234 CANCELLED`).
- `--kill-on-invalid-dep` and `afterok`/`aftercorr` stay on the task
  arrays: dependents of a failed task must not run.

## 9. What changes elsewhere

- `remake resubmit`: deprecated (removed at 1.0); reconcile runs first,
  and the import upserts the new job id under the same run_seq.
- docs: `docs/guide/slurm.md` gains "When jobs are killed or lost";
  `compatibility.md` lists the new files, the table, `slurm-finish`'s
  arguments, and what freezes at 1.0 (the `--json` outcome keys, the
  `Slurm<State>:` reason prefix); `cli.md` the new columns and keys;
  CHANGELOG; old-DB fixture tests cover the 1.2 table, and a 0.10 remake
  still opens a migrated DB.

## 10. Out of scope (deferred)

A SIGTERM handler that records TIMEOUT/scancel from the worker; `--requeue`
support (`_sbatch_opts` can't emit a bare flag today); automatic retry of
NODE_FAIL; refusing local runs of tasks queued on SLURM (we only warn); a
submission lock for two concurrent `run -E slurm`; removing `resubmit`
(1.0).

## 11. Order of work

0. **Facts first**: from the soak archive (done: §2, implementation.md)
   and one live sacct sample on JASMIN (a small array with a completed, a
   failed, a timed-out, a scancelled-running and a cancelled-pending
   element, and an aftercorr dependent): Start/NodeList, collapsed and
   missing rows, `SLURM_TIME_FORMAT`, `-S`, requeue, sacct on a compute
   node. The simulator is built from these, not from memory.

Phase 1:
1. Test framework skeleton (testing.md): the simulated cluster and one
   scenario, before the code it tests.
2. Per-submission jobids files (atomic), `%A_%a` outputs, readers, pruning.
3. `sacct_snapshot` + parsing tests (archive and live fixtures).
4. `slurm_submission` table (schema 1.2), import, the killed-record write.
5. `reconcile` + scenario tests for every outcome.
6. Callers (`run`, commands) + reporting + golden snapshots.
7. Hypothesis state-machine tests.
8. Docs; live JASMIN tests.

Phase 2:
9. `slurm-finish` command and job, chain mode, `-Q` carried, tests.
10. Live JASMIN run of the soak scenario.

The advisor reviews the record format (2, 4), reconcile (5) and the
continuation change (9) before each is committed.
