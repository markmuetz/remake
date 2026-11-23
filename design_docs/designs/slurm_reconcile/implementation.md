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
- **Real sacct output from the soak** (run 81, JASMIN, `sacct` with steps
  and `-P`), copied to `tests/slurm/fixtures/sacct_run81_node_fail.psv`
  with the rule names replaced (`rule_a`…`rule_h`). What it shows:
  - never-started elements have `Start = None` and `NodeList = None
    assigned`; pending ones `Start = Unknown`, `End = Unknown`; running
    ones `End = Unknown`;
  - pending and cancelled-before-start elements are collapsed:
    `6133151_[0-33]`, `6133152_[0]`;
  - NODE_FAIL has `ExitCode 1:0` and a real Elapsed/NodeList;
  - the `Reason` column is the scheduling reason (`Dependency`, `None`),
    even on NODE_FAIL rows: useless for the job's fate, as the advisor
    expected;
  - steps (`.batch`, `.extern`) appear without `-X`.
- **Advisor review of the plan (same day).** Folded into plan.md and
  testing.md. The main changes:
  - the outcome table is now total and state-agnostic;
  - "started" also uses NodeList and the `%A_%a.out` file;
  - `superseded` is split from `done`, and `expired` added, so the gate
    can always settle;
  - legacy pointer files are settled on upgrade, never written as killed;
  - the killed-record write is a new strict-guard upsert that writes
    history only when it applied, with UTC-normalised times;
  - reconcile never allocates a run_seq;
  - chain mode is defined as "never submit a key twice in one chain",
    with chain members found by `parent_run_seq`;
  - the end job's arguments are a compatibility contract, and it carries
    the original `-Q` (today's continuation drops it: an existing bug);
  - the simulator uses dumb bash shims and models `squeue -r`, MinJobAge,
    rejected dependencies and env restore.
- **sacct drops never-started elements.** From the archive: the aftercorr
  dependents of the killed 6133148_11/12/15 (6133150_11/12/15) have no
  sacct rows at all; only 6133150_16 appears (CANCELLED, Start None). "No
  row" therefore can't mean "unknown" forever. The plan classifies it as
  `not_started` when the job has left squeue, sacct has rows for the job,
  and there is no `.out` file. To confirm live.
