# Implementation log

A running record of difficulties, surprises and changes of plan while
implementing [plan.md](plan.md) and [testing.md](testing.md). Newest last.
Each entry: what happened, why, and what was done (and whether the plan
changed).

## 2026-11-23: planning

- The proposal (soak finding 7, MM) was reviewed by a Fable advisor
  before any code. Main corrections to the first idea: lazy
  reconciliation in `info`/`why` can't work alone, because a resubmission
  overwrites `jobs/<rule>.jobids.json` (seen in the soak: run 82 replaced
  run 81's evidence for four rules); switching the continuation to
  `afterany` alone would loop on deterministic failures; "no result and
  not in squeue" isn't enough to call a task lost (late sidecars, sacct
  lag), so only a terminal sacct state counts.
- Claude's first suggestion was to record submissions and task starts in
  the DB at submission time. The advisor's alternative (per-submission
  job-id files; start evidence from sacct) needs no worker change and
  was adopted; a `slurm_submission` table records only reconcile's
  results.
- Soak note 8 (`uses=` reformat counted as a change) turned out not to be
  textual hashing: `uses=` is already a normalised AST. The real cause is
  still to be found from the stored hashes. MM: docstrings keep counting.
