# SLURM reconciliation: lost tasks and the end-of-submission job

**Status: Agreed (plan under advisor review), 2026-11-23. Release 0.11.0,
item 9 (phases 1 and 2).**

A node failure in the 0.11 soak killed SLURM array elements that never
recorded anything, and every remake command misreported what had happened
(design_docs/records/soak_0_11.md, finding 7). This folder holds the work
that fixes it.

| Doc | What it is |
|---|---|
| [plan.md](plan.md) | The design and phased plan: records, `reconcile`, reporting, the end-of-submission job. Settled before implementation; reviewed by a Fable advisor. |
| [testing.md](testing.md) | The test framework: a simulated SLURM cluster, scenario tests, Hypothesis state-machine tests, and live tests on JASMIN. |
| [implementation.md](implementation.md) | A running log of difficulties, surprises and changes of plan during implementation. |

Decisions taken by MM (2026-11-23), from the advisor review:

1. A killed task is recorded as **failed** with a reason, not a new status.
2. A task that never started (cancelled because a dependency failed) gets
   **no task record**, only a skipped count.
3. The end-of-submission job is submitted **every** submission.
4. No automatic retry of node failures (deferred).
5. `remake resubmit` is to be **retired** at 1.0 (`run` replans and skips
   queued rules).
6. Read-only commands (`info`, `why`, `task-info`, `slurm-status`) **may**
   write killed-task records, as they already ingest sidecars; only when
   something is unreconciled.
7. Phases 1 and 2 both go into **0.11**.
8. **Docstrings count** in code hashes (`uses=` and rule code): unchanged.
