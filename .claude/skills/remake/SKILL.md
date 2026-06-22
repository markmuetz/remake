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
remake run <remakefile> [-E singleproc|multiproc|slurm|dask|mod:Class] [-j nproc] [-Q query] [-f|--force] [-I|--ignore-code-changes] [-n|--dry-run] [--check-outputs]
remake set-state <remakefile> -Q query (--success [--check-outputs] [--no-cascade] | --pending) [-n]
remake info <remakefile> [-Q query] [-t|--tasks] [-F|--show-failures] [--json]
remake ls-tasks <remakefile> [-Q query] [--json]   # enumerate tasks/keys (no DB reads)
remake lint <remakefile> [--json]                  # check input/output wiring between rules
remake task-info <remakefile> <selector> [--json]  # one task: status, paths, log, SLURM job
remake task-log <remakefile> <selector> [--path]   # print a task's log (or its path)
remake why <remakefile> <selector>                 # explain rerun decision for one task
remake slurm-status <remakefile> [--json]          # live queue state per rule
remake run-task <remakefile> <key-or-prefix>       # run one task by key
remake run-array-task <remakefile> <rule> <idx>    # internal: SLURM payload
remake resubmit <remakefile>                       # re-run .remake/submit.sh, no replanning
remake version
```

`<selector>` = a task key prefix, or `-Q '<query>'` resolving to exactly
one task — so you never need `info -t` just to find a key.

**Prefer `--json`** (`info`, `task-info`, `slurm-status`) over parsing
the aligned-text output. Logs go to stderr; stdout is data only.
Exit codes: `run` exits 1 if any task failed (it still runs the rest —
check `info -F` for what and why); `lint` exits 1 on wiring problems. Global
flags before the subcommand: `-D` debug / `-W` warnings-only, `-X`
debugger on exception.

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

1. `remake info <file> --json` — which rules have failures.
2. `remake info <file> -F` — every failed task: traceback, timestamp and
   per-task log path. For one task: `remake task-info <file> <selector>`
   (adds input/output existence and the SLURM job id + array index).
3. More context: `remake task-log <file> <selector>`; for SLURM runs the
   scheduler stdout/stderr is `.remake/slurm/output/<rule>/<idx>.out|.err`
   with the index from `task-info`.
4. SLURM resource kills do NOT leave tracebacks. Get the job id from
   `remake task-info` (or `slurm-status`), then
   `sacct -j <jobid> --format=JobID,State,ExitCode,Elapsed,MaxRSS`:
   `OUT_OF_MEMORY` → raise `mem`, `TIMEOUT` → raise `time` (per-rule
   `config={'slurm': {...}}`).

**Classify before fixing** — the fix path differs:
- *Code bug* (traceback in rule code) → fix the rule; the code change
  itself triggers the rerun.
- *Environment* (ImportError/ModuleNotFoundError in job) → the job env
  lacks a package; fix the env the sbatch scripts run in, then
  `remake run` again (failed tasks rerun automatically).
- *`remake` not found in the job* (`remake: command not found`, every
  element `FAILED 127:0` at `00:00:00` elapsed, `info` shows 0 success/0
  failed — nothing recorded) → the per-rule sbatch runs bare `remake
  run-array-task …` with no env activation, so `remake` must be on the
  compute node's PATH. SLURM propagates the *submit-time* PATH
  (`--export=ALL`), so submit from an env where remake is actually
  installed/on PATH — a dev checkout shadowed only via `PYTHONPATH` does
  NOT carry over to the job.
- *Resource kill* (sacct, no traceback) → raise mem/time in slurm config.
- *Missing-input cascade* → find the most-upstream failure and fix only
  that; downstream failures clear themselves on the next run.

## SLURM monitoring

- DB-side progress: `remake info <file> --json` (success/failed/pending
  per rule).
- Queue-side: `remake slurm-status <file> [--json]` — per rule: last
  submitted job id, element state counts (PD/R), and pending reasons.
  "not in queue" + tasks still pending in `info` = the job finished or
  died; check `info -F`, then sacct.
- Pathologies surfaced in slurm-status's reasons column:
  - `DependencyNeverSatisfied` — upstream element failed; the dependent
    element will never start. Fix upstream, `remake run -E slurm` again
    (replans, resubmits what's needed; queued rules are skipped whole).
  - Continuation job (`remake_continue`) stuck pending forever — its
    afterok upstream failed. Same fix.
  - Instant-fail on submission (`Invalid qos`, partition errors) — check
    `partition`/`qos`/`account` in `Remake(config={'slurm': {...}})`.
- Elements `COMPLETED 0:0` in sacct but still `pending`/`to run` in `info`,
  and `why` says "never recorded in DB" → task-key mismatch: a kwarg value
  isn't JSON-round-trip-stable (a `tuple`/`dict`/nested value reloads from
  `.remake/jobs/*.json` as a `list` on the compute node, so `Task.key`
  differs from plan time). Sidecars record under a key the planner never
  queries, and outputs built from the kwargs may be mis-named. Passes every
  *local* run (no JSON round-trip); only shows on SLURM. Fix: make matrix/
  kwarg values plain JSON scalars (encode richer values as a canonical
  string) — see references/remake2_to_remake3.md.
- `remake run <file> -E slurm` is idempotent: complete tasks are skipped,
  queued rules are skipped, only the gap is submitted. `remake resubmit`
  re-executes the last submit.sh verbatim (after a cluster outage).

## Why did/didn't a task rerun?

**`remake why <file> <selector>`** answers this directly: will-run
yes/no plus the applicable reasons (never run / failed / code changed
with diff / uses changed / outputs incomplete / upstream propagation,
in-pass or durable `upstream-newer`).

Background for interpreting its output — the planner's checks, in order:

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
7. Upstream propagation (in-pass): an upstream task reran *this pass*.
   Element-wise if this rule shares the upstream's matrix (only the
   matching element reruns); otherwise conservative (ALL tasks rerun —
   fan-in pays this).
8. Upstream propagation (durable / cross-pass): an upstream committed in
   a *later invocation* than this task without rerunning it in the same
   pass — e.g. you ran the upstream alone with `run -Q`, or a crash
   killed the consumer before it ran. Caught by a persisted per-run
   counter (`run_seq`): a task reruns when an upstream's stamp is greater
   than its own. `why` reports this as **upstream-newer** ("an upstream
   ran more recently … output may be stale"). Without it, settling an
   upstream alone would silently strand the consumer as falsely
   up-to-date.

Common surprises: renaming a kwarg or changing matrix values changes
task keys → everything looks never-run (rule 2 may rescue via outputs);
editing a `uses=` helper reruns every task of the rule (rule 5);
touching a file does NOT trigger reruns (no mtime checks, ever); a
stateful object in `uses=` whose `repr` embeds mutable state — above all
a loguru/`logging` logger (its repr carries handler id/level/sink) —
reruns every task whenever that state differs between invocations, so
"uses= changed" with no code change usually means a logger (or DB
connection / open file) is in `uses`. Keep them out and import locally —
see references/authoring.md.

## Task status

"What state is X in right now" (vs. `why`'s "what would the planner
do"): `remake task-info <file> <selector> [--json]` — status +
timestamp, input/output paths with on-disk existence, per-task log
path, SLURM job id + array index, traceback if failed. For many tasks
at once: `remake info <file> -t --json` with `-Q` to narrow.

## Query crafting (-Q)

Queries are Python expressions evaluated against each task's kwargs
**plus `rule`, the rule name**: `-Q 'year > 1985 and model == "era5"'`,
`-Q 'rule == "extract"'`, `-Q 'rule in ["extract", "clean"] and year ==
2010'` (shell: single-quote the whole expression). `rule` is how you
target whole rules — including tasks with no matrix kwargs at all — and
how you disambiguate rules sharing a matrix. A task whose rule lacks a
referenced kwarg silently doesn't match — filtering `year == 2000` never
touches a fan-in rule with no `year`. Habits: preview with `remake info
-Q ...` or `run -n -Q ...` before running; `--force -Q` is the surgical
rerun tool — keep the query tight.

## State control

`run -I` runs only tasks that have never *succeeded* (code/uses changes
ignored; failed tasks rerun; upstream reruns still propagate, so
fan-ins stay correct) — the gap-filler after editing a pipeline.
`set-state -Q ... --success [--check-outputs]` records success without
running anything (migration adoption: `set-state file -Q True --success
--check-outputs`); `--pending` deletes records — but note complete
outputs are re-adopted by the default check_outputs mode, so to force a
rerun use `run --force` instead.

`--success` stamps the current `run_seq` (see rerun reason 8), so a
settled task becomes the newest in the graph and no upstream out-ranks
it. By default it also **cascades**: downstream complete tasks are
re-stamped too, so settling a mid-graph task doesn't leave its
descendants looking stale and rerunning. A guard skips any descendant
that has an *independently newer* upstream (a diamond where another
branch genuinely changed), so cascade never swallows a real rerun. Use
`--no-cascade` to stamp only the selected tasks. Because `--success`
makes a task newer than its upstreams, it's also the escape hatch for
the conservative cross-pass rerun: if an upstream reran but its output
didn't actually change, `set-state -Q 'rule == "consumer"' --success`
stops the spurious rebuild.

**The fix-one-failure idiom.** proc[n=42] of 100 failed; the user edits
the rule code to handle it. A plain `run` now wants all 100 (code
changed). Two repairs, with different downstream behaviour:
- `run -Q 'n == 42'` — surgical: reruns just that task (it's failed, no
  -f needed), but downstream tasks *not matching the query* (a fan-in
  with no `n`) stay unrun until a later unfiltered run.
- `run -I` — runs everything never-succeeded: the failure AND its
  previously-skipped downstream, to completion, in one invocation.

Either way the 99 successes still carry the old code hash, so the next
plain `run` would rerun them all — the repair *defers* the code-change
rerun, it doesn't cancel it. To assert the code change doesn't
invalidate them, re-stamp: `set-state file -Q 'rule == "proc"'
--success` (records success with the *current* hashes; add
--check-outputs to verify against disk first). Skipping this is safe
but causes a surprise mass-rerun later.

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
