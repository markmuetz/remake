# Plan: reconciling SLURM submissions

**Status: Agreed in outline by MM (2026-11-23); this text under Fable
advisor review before implementation.** Background: soak finding 7
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
- **Element outcome** (what reconcile decides, per element):

| Outcome | Evidence | Written to the DB |
|---|---|---|
| `done` | a task record (from the worker's sidecar) with run_seq ≥ the submission's | nothing new |
| `queued` / `running` | squeue shows the element in a non-terminal state (incl. requeued, held) | nothing |
| `killed` | sacct: terminal state in KILLED_STATES, with a start time; no record ≥ run_seq | FAILED record + history row, reason `Slurm<State>: …` |
| `not_started` | sacct: terminal (CANCELLED, BOOT_FAIL…) with no start time; no record | nothing (counted as skipped) |
| `result_missing` | sacct: COMPLETED, no record after a grace period | nothing; reported (quota, quarantined sidecar) |
| `unknown` | squeue or sacct failed, or sacct shows no row / a non-terminal state while squeue doesn't list the element | nothing; reported as unknown |

KILLED_STATES: FAILED, NODE_FAIL, TIMEOUT, OUT_OF_MEMORY, CANCELLED (when
started), PREEMPTED, DEADLINE. A FAILED element with no sidecar is a
failure before the task ran (import error, bad index): still `killed`,
reason `SlurmFailed: exit <code>, no result recorded`.

- **Reconciled**: a submission is reconciled when no element is `queued`,
  `running` or `unknown` and no `result_missing` is still inside its grace
  period. Reconciled submissions are never queried again.

Rule: **unknown is never lost.** Anything short of a terminal sacct state
with no result is `unknown`, and nothing is written.

## 3. Records (on-disk contract additions)

1. **Per-submission job-id files.** `submit.sh` writes
   `jobs/<rule>.<run_seq>.jobids.json` (`{"v": 1, "slurm_array_job_id",
   "run_seq", "submitted"}`) and keeps `jobs/<rule>.jobids.json` as the
   "latest" pointer (same content). Pruned with the matching spec file.
   The continuation/end-of-submission job's id goes in
   `jobs/_finish.<run_seq>.jobids.json`.
2. **Job output names include the job id**: `%A_%a.out/.err`. Readers
   (`_failed_line`, `task-log`) look up the file for the task's last
   submission's job id, falling back to `%a.*` for old submissions.
3. **DB: a `slurm_submission` table** (schema 1.2, additive):
   `(rule_id, run_seq, jobid, n_tasks, submitted, reconciled TIMESTAMP NULL,
   counts JSON)`. Filled from the per-submission files at the start of
   reconcile; `reconciled` set once the submission is settled; `counts`
   holds `{done, failed, killed: {NODE_FAIL: 4, …}, not_started,
   result_missing}` for `history --runs` and `slurm-status`. It makes the
   gate ("anything unreconciled?") a single query.
4. **Killed-task records** go through the ingest path's guarded upsert at
   the **submission's** run_seq, with a strict guard: written only if the
   task has no record with run_seq ≥ the submission's. A worker sidecar
   ingested later (run_seq ≥) still overwrites it, so a late real result
   always wins. History rows use sacct's End as `finished` (the unique
   index makes repeats idempotent); `host` = NodeList, `wall_s` = Elapsed,
   `error` = the reason text. The reason starts with an exception-like word
   so `error_signature` keeps it: `SlurmNodeFail: job 6133148_11 on
   host1234 after 13:40 (run 81)`.
5. Optionally, workers add `slurm_job` (`$SLURM_ARRAY_JOB_ID_$SLURM_ARRAY_TASK_ID`)
   to their result sidecar, recorded in `task_run`, so history shows which
   job ran it. (Nice to have; not needed for reconcile.)

## 4. `sacct_snapshot`

Beside `squeue_snapshot`, in `executors/slurm_executor.py` (or a new
`executors/slurm_query.py` if the module gets long):

```
sacct -X -n -P -j <id1>,<id2>,… \
      --format=JobID,State,ExitCode,Start,End,Elapsed,NodeList
```

- Returns `{base_id: {index: Element(state, start, end, elapsed, node, exit)}}`.
- Parses collapsed never-started elements (`6133152_[0-3,7%4]`) into
  indices; `CANCELLED by 12345` → `CANCELLED`; `Start = Unknown/None` →
  not started.
- Raises `SacctError` on a non-zero exit, a timeout, or output it can't
  parse. Callers turn that into `unknown`.
- Batched (one call per reconcile, chunked if the id list is long).
- To verify on JASMIN before relying on it: field formats, collapsed
  ranges, requeued elements (latest state without `-D`), slurmdbd lag just
  after a job ends, sacct on compute nodes.

## 5. `reconcile`

One idempotent function, `reconcile(rmk, *, run_seq=None, write=True,
grace=…)` in a new module `core/reconcile.py` (or `executors/`), used by
every caller:

1. Ingest sidecars (as today).
2. Load submissions: per-submission files not yet in `slurm_submission`
   are added; select the unreconciled ones (all, or those at `run_seq`).
   If none: return immediately (no SLURM calls).
3. Snapshot squeue **before** reading task records, then ingest again
   (a result that lands in between is seen as done, never as lost).
4. For elements not done and not in squeue: one sacct call.
5. Classify each element (§2). Write killed records (§3.4) if `write`.
6. Update `slurm_submission.counts`; set `reconciled` when settled. Stamp
   the run's counts (`n_failed` etc.) for runs whose submissions are all
   reconciled.
7. Return a report: per submission and per element, used by the commands.

Guarantees (each a test invariant, see testing.md):

- Never classifies an element `killed`/`not_started` without a terminal
  sacct state and no result.
- Never overwrites a record at run_seq ≥ the submission's.
- Writes nothing when squeue or sacct fails.
- Twice is the same as once.
- Only reads a submission's own files (a later submission of the same
  rule doesn't confuse an earlier one).

## 6. Callers

- **`run` (any executor)** reconciles before planning, so a killed task
  has a FAILED record and is rerun (fixing the correctness cases in §1).
  Off-cluster (no squeue/sacct) with unreconciled submissions: warn
  once, plan with what the DB says.
- **The end-of-submission job** (phase 2) reconciles its own run_seq.
- **`slurm-status`, `info`, `why`, `task-info`**: reconcile first, gated
  on unreconciled submissions; `SqueueError`/`SacctError` → a warning and
  `unknown`, not a crash (`slurm-status` crashes on `SqueueError` today).

## 7. Reporting

- `slurm-status`: per rule's latest submission, columns `tasks, PD, R,
  done, failed, killed, not started, reason`; a killed breakdown by state
  (`NODE_FAIL 4`) in the reason column or under the table. `--json` gains
  `killed` (by state), `not_started`, `result_missing`, `unknown`.
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

- Submitted with **every** submission (decision 3), `--dependency=afterany`
  on all the submission's job refs (`afterany` never becomes invalid, so
  it is never killed by `kill-on-invalid-dep`). Resources as the
  continuation today (10 min, 1G).
- Runs `remake slurm-finish <remakefile> --run-seq N` (name to settle;
  hidden or documented as internal):
  1. `reconcile(run_seq=N)`, retrying for up to ~2 min while sacct still
     shows non-terminal states (slurmdbd lag); what's left stays
     unreconciled for the next caller.
  2. If deferred rules exist: plan and submit the continuation **in
     chain mode** — tasks that failed or were killed in this chain
     (run_seq ≥ `REMAKE_PARENT_RUN_SEQ`) and everything downstream of
     them are excluded (logged: `excluded: N tasks failed in this chain;
     rerun with remake run`), so a deterministic failure is not resubmitted
     in a loop.
  3. Logs a one-line submission summary.
- If it is itself lost, cancelled or never runs, nothing is wrong: the next
  `run`/`slurm-status` reconciles (the backstop).
- `--kill-on-invalid-dep` and `afterok` stay on the task arrays (dependents
  of a failed task must not run).

Open for phase 2: whether the chain exclusion needs a planner option
(`exclude_failed_since`) or can be done by filtering the plan; how it
interacts with rules skipped as already queued.

## 9. What changes elsewhere

- `remake resubmit`: documented as deprecated (to be removed at 1.0);
  reconcile runs first so its submissions are at least recorded.
- `prune_spec_files` also prunes per-submission jobids files and old
  `%A_%a` outputs of pruned submissions.
- docs: `docs/guide/slurm.md` gains "When jobs are killed or lost";
  `compatibility.md` lists the new files and table; `cli.md` the new
  columns and JSON keys; CHANGELOG.

## 10. Out of scope (deferred)

A SIGTERM handler that records TIMEOUT/scancel from the worker; `--requeue`
support; automatic retry of NODE_FAIL; guarding local runs against tasks
queued on SLURM; a submission lock for two concurrent `run -E slurm`;
retiring `resubmit` (1.0).

## 11. Order of work

Phase 1:
1. Test framework skeleton (testing.md): the simulated cluster and one
   scenario, before the code it tests.
2. Per-submission jobids files, `%A_%a` outputs, readers updated.
3. `sacct_snapshot` + parsing tests.
4. `slurm_submission` table (schema 1.2) and the guarded killed-record write.
5. `reconcile` + scenario tests for every outcome.
6. Callers (`run`, commands) + reporting + golden snapshots.
7. Hypothesis state-machine tests.
8. Docs; live JASMIN tests.

Phase 2:
9. `slurm-finish` command and job, chain-mode continuation, tests.
10. Live JASMIN run of the soak scenario.

Each step: review by the advisor at its judgement of risk (the record
format, reconcile, and the continuation change certainly).
