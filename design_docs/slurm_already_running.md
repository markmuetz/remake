# SLURM "is this task already running?" detection

A design note for the duplicate-submission guard. Builds on
[slurm_implementation.md](slurm_implementation.md) (sidecar/ingest result
recording) and the SLURM executor in `src/remake/executors/slurm_executor.py`.

## The problem

SLURM results land in the DB lazily: `run-array-task` writes a sidecar under
`.remake/tasks/results/...`, batch-ingested by the *next* plan/run/info. So
while an array is still PD/R, the task it is computing has **no DB record and
no output yet**. A second `remake run -E slurm` — a fat-fingered re-run, an
impatient user, a cron, a continuation job racing the array — will see "needs
running" and **resubmit a duplicate**. Two array elements then race on the same
output path: wasted compute at best; a half-written or interleaved output, or a
confused DB, at worst. On a wide array this is silent and expensive.

## What already exists

Detection is built, but at **rule** granularity:

- `squeue_snapshot()` — one `squeue -h -r -u $USER -o '%i %t %r'` call,
  returning `{base_jobid: [(elem_id, state, reason)]}`. Returns `{}` when
  squeue is missing or errors.
- `_active_jobids()` — base ids with any element in `PD`/`R`/`CF`.
- `_queued_jobids(rule, active)` — reads the rule's last-submission sidecar
  `.remake/jobs/<rule>.jobids.json` and intersects with the active set.
- `run_tasks()` — if a rule has any still-active job, it **skips the whole
  rule** this run (rewriting its `<rule>.json` spec would corrupt the array
  indices the queued jobs read) and wires downstream dependencies to the
  literal queued job ids.

This is conservatively correct but coarse, and it leans entirely on the
*last-submission sidecar* being the source of truth for "what is that job
doing." It cannot answer "which remake task is array element `1234_57`?"
without re-deriving it from `<rule>.json` by position — and that breaks if the
spec was rewritten by an intervening replan.

## The gap, and the proposed mechanism

Two improvements, in order of value:

1. **Robust queue → task mapping.** Stamp each submitted job with enough
   identity to reconstruct *which remake task* it is, straight from a queue
   snapshot, independent of the last sidecar:
   - bake a **run id** and the **job-spec path** into the job (via
     `--comment`, e.g. `remake:<runid>:<rule>:<specpath>`, and a stable
     `--job-name` that already carries the rule). `--comment` survives in
     `squeue -O Comment` / `scontrol show job`.
   - array element index → task is then `specs[index]` of *that submission's*
     spec file (pinned by run id), not whichever `<rule>.json` happens to be
     on disk now.
   This feeds `slurm-status` (richer, replan-proof mapping) and the planned
   submit-time message ("submitted rule X: N tasks, job 98765").

2. **Per-task skip instead of per-rule.** Once a job carries its run id +
   index, the planner can skip *exactly* the tasks currently in flight and
   submit the rest as a fresh array — rather than parking the whole rule until
   the entire previous submission drains. (Caveat: a partial array resubmit
   needs a fresh spec file under a new run id so it doesn't disturb the indices
   the in-flight array is still reading — cheap, since specs are per
   submission.)

   *Update 2026-07-10:* specs **are** per submission now
   (`.remake/jobs/<rule>.<run_seq>.json`, immutable, pinned by the sbatch
   script via `run-array-task --specs`; the jobids sidecar records the
   run_seq). The spec-rewrite corruption this doc worried about is
   structurally impossible, and the caveat above is satisfied — per-task skip
   only needs the queue→task mapping (item 1) and the planner-side change.

Job-id reuse (Slurm recycles ids past `MaxJobId`) and cross-run name
collisions (same rule name, different working dir / previous run) are exactly
why we match on **run id + spec path**, not on a bare job id or job name.

## Is squeue trustable? (yes — with two rules)

We want to assume squeue is trustable. Here is the case against, and why it
still holds.

**Reasons it might not be:**

1. **Eventual consistency.** squeue reflects slurmctld's in-memory state; a
   just-submitted job may not appear immediately and a just-finished one may
   linger or vanish. There is a stale window.
2. **squeue can fail or be throttled.** On a busy controller squeue times out,
   errors, or is rate-limited. Our `squeue_snapshot()` swallows that and
   returns `{}` — i.e. **a failed squeue is indistinguishable from an empty
   queue**, which would currently green-light resubmitting everything. This is
   the one genuinely dangerous case.
3. **Completed-but-not-ingested gap.** A job can leave the queue microseconds
   before its sidecar is written/ingested. squeue truthfully says "not
   queued," the DB says "no record" → resubmit while the output write is
   in flight. squeue being *accurate* does not help here at all.
4. **Queue purge.** Finished jobs age out fast; squeue alone can't tell "never
   ran" from "ran and finished" — that needs sacct, heavier and slower.
5. **Federation / multi-cluster.** squeue defaults to the local cluster; a job
   on a federated partition may not show.

**Why we trust it anyway:**

- **squeue is a hint, not the correctness mechanism.** The real defence
  against duplicate *output* is at the filesystem/DB layer: outputs written
  atomically (temp + rename), and the planner not re-running a task whose
  output already exists and is fresh. The queue check is a "don't bother
  re-submitting / don't waste a slot" optimisation layered on top. It only has
  to be *usually* right, so points 1, 3 and 4 — all sub-second-to-seconds
  windows against human/cron-paced runs — are acceptable: a missed detection
  costs one duplicate run, not corrupted data, and self-corrects on the next
  plan once the output and sidecar exist.
- **A wrong "still running" is free.** A false positive just defers a task to
  the next run; it is picked up later. The asymmetry favours trusting squeue.
- **We already gate on state**, not mere presence (PD/R/CF only), so we are not
  trusting squeue loosely.
- **The one unsafe case (point 2) is fixable without needing squeue to be more
  reliable.** Distinguish "squeue ran and returned nothing" from "squeue
  failed": on failure, do **not** treat the queue as empty — either refuse to
  submit (clear error, suggest `--force`) or treat the recorded-active job ids
  as still active. That converts the dangerous false-negative into a safe
  conservative one.

**Conclusion — trust squeue, under two rules:**

1. **Never conflate squeue-failure with an empty queue.** `squeue_snapshot()`
   must signal failure distinctly (e.g. raise / return a sentinel), and the
   caller must fail safe rather than resubmit-all.
2. **Keep duplicate-safety as defence-in-depth** at the output/DB layer, so a
   missed detection is wasted compute, never wrong data.

The expensive alternatives — sacct cross-checks, a persistent reconciled
job-state DB — buy a marginal reliability gain for a lot of machinery, and are
not worth it given the above.

## Concrete changes

- `squeue_snapshot()`: separate "ran, empty" from "failed" (sentinel or
  exception); callers fail safe on failure. *(fixes the latent resubmit-all
  bug)*
- Stamp jobs with `--comment=remake:<runid>:<rule>:<specpath>`; persist the run
  id with the submission (alongside `<rule>.jobids.json`).
- Map queue elements → tasks via run id + spec path + index; share this with
  `slurm-status`.
- Submit path: per-task skip; log skipped-because-queued tasks; emit the
  per-rule submit summary line (the UX todo).

## Open questions

- `--comment` length limits / availability across Slurm versions — fall back to
  a per-submission manifest keyed by job id if `--comment` is unreliable.
- Continuation jobs: the replanning continuation job *is itself* a queued job
  that will resubmit — make sure it is excluded from / aware of the guard so it
  doesn't trip on the array it is meant to follow.
- Do we ever want `--force` to override and resubmit an in-flight task (e.g.
  after a node failure left a zombie), or is `scancel` + re-run always the right
  manual path?
