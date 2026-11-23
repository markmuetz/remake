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
- **Live probe (plan step 0), JASMIN.** Plain sbatch arrays covering each
  outcome; results and findings in
  `tests/slurm/fixtures/jasmin_probe_2026-11-23/README.md`. It confirmed
  the archive: never-started elements often have no sacct row of their
  own. It also showed that sacct labels an aftercorr array's unsplit
  pending remainder with a single index (`J_5`, unbracketed), so a
  Start-less row is evidence about no element in particular. The plan's
  `not_started` rule now rests on squeue alone (`-r` lists every pending
  element) plus "never started". Also: `SLURM_TIME_FORMAT=%s` works; sacct
  runs on compute nodes; requeue is disabled on JASMIN (`scontrol
  requeue`: "presently disabled"), so requeue stays a simulator-only case;
  finished elements leave squeue within seconds.

## 2026-11-23: steps 2-4, and the advisor's review of step 2

- Steps 2 (per-submission records, `<jobid>_<index>` output names), 3
  (`sacct_snapshot`, parsed against the archive and probe) and 4
  (`slurm_submission`, schema 1.2; the strict-guard killed write) are in.
  The schema bump broke one test that hard-coded 1.1; it now compares with
  `SCHEMA_VERSION`.
- **Advisor review of step 2.** Taken:
  - `not_started` was a permanent verdict built on missing evidence
    (squeue not yet listing a new job; sacct and the shared filesystem
    lagging a short-lived element). It now needs corroboration or a start
    grace (plan §2).
  - Start evidence from `.out` uses the exact `<jobid>_<index>` name and
    an mtime check. `element_output` no longer falls back from the new name
    to the old one: a stale `<index>.err` could show another task's error
    in `info --failed`. The old name is used only for a pre-0.11
    submission (pointer without `v`).
  - The `resubmit` guard's regex missed rules when the path was quoted (a
    metadata dir with a space), so the guard passed silently. Fixed and
    tested.
  - Temp files are named per run_seq (two runs may submit the same rule);
    `id;cluster` from `sbatch --parsable` is stripped; stale `.tmp` files
    are pruned; prune stays after reconcile (comment added).
  - Simulator: `sacct` now prints only the jobs asked for with `-j` and
    rejects an unexpected `--format`; lag for start (not yet split out),
    squeue listing and `.out` visibility; non-array jobs render as `J`
    without `SLURM_ARRAY_*`; the clock is based on real time; payload
    stdout/stderr go to the `.out`/`.err` files; `kill(partial=True)`;
    exit codes as in the probe.
- **The aftercorr remainder row belongs to the array's highest index.**
  The advisor noted that the archive keeps a row for 6133150_16 (CANCELLED,
  never started), which the simulator didn't. Reading the probe again: at
  t15, `B_5` stood for 0, 2, 3 and 5; B_5 then ran, so the final sacct has
  no remainder row for B. In the archive, `_16` never ran and kept the
  CANCELLED row. So the row is held by the array's last index, and becomes
  that element's own row if it starts. Modelled, and checked by replaying
  the probe's A-D sequence through the shims against
  `sacct_epoch_final.psv` (matched first time).
- Not taken: pointing `-e` at the `-o` file to halve the output files.
  The files are pruned after 7 days; noted for the docs instead.
- Requeue is disabled on JASMIN, so the requeue scenarios rest on the
  simulator alone.
