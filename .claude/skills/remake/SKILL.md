---
name: remake
description: >
  Operate, debug and author remake pipelines (remakefiles). Use when the
  user asks about remake tasks/rules, pipeline failures, SLURM runs
  submitted via remake, why tasks rerun (or don't), task status, writing
  or modifying @rule pipelines, or migrating remake2 remakefiles to
  remake3.
---

# remake

remake is a make-like build tool for file-based science pipelines: rules
(Python functions + `@rule` decorator) expand over parameter matrices
into tasks, completion is tracked in a per-directory `.remake/` metadata
store, and execution is local or via SLURM array jobs.

**Operating rule: CLI-first.** Use `remake` CLI commands for every
workflow below. Reading `.remake/` files directly is a last resort
(read-only — NEVER write to `.remake/`), and each time it's needed,
tell the user: that's a CLI gap worth reporting upstream.

## CLI surface

```
remake run <remakefile> [-E slurm|singleproc|mod:Class] [-Q query] [-f|--force] [-n|--dry-run] [--check-outputs]
remake info <remakefile> [-Q query] [-t|--tasks] [-F|--show-failures]
remake run-task <remakefile> <key-or-prefix>      # run one task by key
remake run-array-task <remakefile> <rule> <idx>   # internal: SLURM payload
remake resubmit <remakefile>                      # re-run .remake/submit.sh, no replanning
remake version
```

Global flags before the subcommand: `-D` debug / `-W` warnings-only
logging, `-X` drop into debugger on exception.

`info` columns: rule, tasks, success, failed, pending, to run. A row of
`?` with "deferred" means a dynamic matrix that can't expand yet
(upstream outputs missing).

## The .remake/ map (read-only reference)

```
.remake/remake.db                          SQLite metadata (never touch directly)
.remake/remake.log                         shared DEBUG log (run/info/resubmit)
.remake/tasks/log/<rule>/<k:2>/<k2:>.log   per-task log, named by 40-hex task key
.remake/jobs/<rule>.json                   SLURM specs: [{task_key, rule, kwargs}]; array index = position
.remake/jobs/<rule>.jobids.json            last submitted SLURM job id(s)
.remake/slurm/<rule>.sbatch, submit.sh     generated scripts
.remake/slurm/output/<rule>/<idx>.out/.err scheduler stdout/stderr, index-named (NOT stable across runs)
```

Task keys are stable: `sha1('<rule>:<sorted kwargs repr>')`. Key prefixes
work anywhere a key is accepted (like git).

## Failure triage

1. `remake info <file>` — which rules have failures.
2. `remake info <file> -F` — each failed task with its stored full
   traceback and timestamp.
3. More context: the per-task log at
   `.remake/tasks/log/<rule>/<key[:2]>/<key[2:]>.log` (CLI gap), and for
   SLURM runs the scheduler files — map key → array index via
   `.remake/jobs/<rule>.json` (CLI gap), then read
   `.remake/slurm/output/<rule>/<idx>.out|.err`.
4. SLURM resource kills do NOT leave tracebacks. Check
   `sacct -j <jobid> --format=JobID,State,ExitCode,Elapsed,MaxRSS`
   (jobid from `.remake/jobs/<rule>.jobids.json`): `OUT_OF_MEMORY` →
   raise `mem`, `TIMEOUT` → raise `time` (per-rule
   `config={'slurm': {...}}`).

**Classify before fixing** — the fix path differs:
- *Code bug* (traceback in rule code) → fix the rule; the code change
  itself triggers the rerun.
- *Environment* (ImportError/ModuleNotFoundError in job) → the job env
  lacks a package; fix the env the sbatch scripts run in, then
  `remake run` again (failed tasks rerun automatically).
- *Resource kill* (sacct, no traceback) → raise mem/time in slurm config.
- *Missing-input cascade* → find the most-upstream failure and fix only
  that; downstream failures clear themselves on the next run.

## SLURM monitoring

- DB-side progress: `remake info <file>` (success/failed/pending per rule).
- Queue-side: `squeue -u $USER -r` ; correlate job ids with rules via the
  jobid sidecars (CLI gap). `-r` expands arrays to one row per element.
- Pathologies to look for in squeue's REASON/NODELIST column:
  - `DependencyNeverSatisfied` — upstream element failed; the dependent
    element will never start. Fix upstream, `remake run -E slurm` again
    (replans, resubmits what's needed; queued rules are skipped whole).
  - Continuation job (`remake_continue`) stuck pending forever — its
    afterok upstream failed. Same fix.
  - Instant-fail on submission (`Invalid qos`, partition errors) — check
    `partition`/`qos`/`account` in `Remake(config={'slurm': {...}})`.
- `remake run <file> -E slurm` is idempotent: complete tasks are skipped,
  queued rules are skipped, only the gap is submitted. `remake resubmit`
  re-executes the last submit.sh verbatim (after a cluster outage).

## Why did/didn't a task rerun?

The planner's logic, in order — walk it top-down and stop at the first
hit (this requires knowing the pipeline source and `remake info -t`
status; there is no CLI explain command yet — known gap):

1. `--force` → reruns.
2. No DB record for the key → reruns, UNLESS check_outputs mode is
   `fallback` (default) or `always` and all declared outputs exist
   complete on disk (this is how a fresh `.remake/` adopts existing
   outputs without rerunning).
3. DB status is failed/pending → reruns.
4. The rule function's source changed *meaningfully* (AST-normalised
   compare: comments/formatting do NOT count) → reruns.
5. Anything in `uses=` changed (functions: AST-normalised; values:
   repr) → reruns.
6. check_outputs `always` only: a declared output is missing/incomplete
   → reruns.
7. Upstream propagation: an upstream task reran. Element-wise if this
   rule shares the upstream's matrix (only the matching element reruns);
   otherwise conservative (ALL tasks rerun — fan-in pays this).

Common surprises: renaming a kwarg or changing matrix values changes
task keys → everything looks never-run (rule 2 may rescue via outputs);
editing a `uses=` helper reruns every task of the rule (rule 5);
touching a file does NOT trigger reruns (no mtime checks, ever).

## Task status

"What state is X in right now" (vs. the planner's "what would run"):
- `remake info <file> -t -Q '<narrowing query>'` — per-task status lines.
- Failed detail: `-F` (traceback + timestamp).
- On disk: check the output paths from the rule definition.
- Queued: squeue + jobid sidecar (as above).
No single-task detail command yet (`task-info` — known gap).

## Query crafting (-Q)

Queries are Python expressions evaluated against each task's kwargs:
`-Q 'year > 1985 and model == "era5"'` (shell: single-quote the whole
expression). A task whose rule lacks a referenced kwarg silently doesn't
match — filtering `year == 2000` never touches a fan-in rule with no
`year`. Habits: preview with `remake info -Q ...` or `run -n -Q ...`
before running; `--force -Q` is the surgical rerun tool — keep the query
tight.

## Authoring rules

Read [references/authoring.md](references/authoring.md) before writing
or modifying a remakefile. Validate work with `remake run <file> -n`
(plan without running) and `remake info <file>`.

## remake2 → remake3 migration

Translate by hand (LLM), don't script it. Read
[references/remake2_to_remake3.md](references/remake2_to_remake3.md)
for the full difference guide and workflow. Core moves: Rule classes →
`@rule`-decorated functions; implicit registration →
`rmk.rules_from_current_module()`; free module globals → `uses=`;
path-matching task DAG → explicit `depends_on`; fresh `.remake/` (the
default check_outputs mode adopts existing on-disk outputs).
