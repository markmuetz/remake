# Interactive tutorial

> **Status: Agreed (MM, 2026-09-25) — prototype (lessons 0–2) next.**
> Milestone: docs work alongside 0.9; not a 1.0 blocker.

## Goal

A tutorial for both returning users (refresh the mental model of today's
remake) and newcomers (no prior remake). Delivered as **docs pages** that
stand alone, plus an optional **interactive layer**: the learner works in
one terminal, Claude runs `/remake-tutor` in another, watches what they do
and gives feedback.

## Lessons

One project (synthetic station data, sites × years — no netCDF deps) grows
across the lessons:

0. Mental model — a task reruns because of its DB record, its code / `uses=`
   / io spec, or an upstream task it reads rerunning. Never mtimes.
1. First rule: `run`, rerun (nothing to do), `info`.
2. Matrix: sites × years, `ls-tasks`, `-Q`.
3. Chaining and fan-in; a task reading the previous year; "declare what you
   read" (path-derived dependencies, review 2026-09-24 H3).
4. Change detection: code edits vs comments/formatting, `uses=`, scope
   warnings, `why`.
5. Failure and recovery: skip of downstream, exit codes, per-task logs,
   `task-info`, rerunning only what failed.
6. The DB and existing outputs: `.remake/`, `check_outputs`, adopting
   outputs with `set-state --success --check-outputs`,
   `--ignore-code-changes` (bug 03's trap).
7. Scaling up: executors, resources, Ctrl-C; SLURM via dry run.
8. (Optional) dynamic matrices, `@deferrable`.

Ends with a cheat sheet and a "why did this rerun?" checklist.

## One spec, three uses

Each step is data (`docs/tutorial/steps/*.yaml` or similar): setup commands,
the learner's commands, a *predict first* question, and the expected
outcome (which tasks run / skip / fail). It drives:

1. the docs page (text + expected output, so they can't drift);
2. a pytest that replays every step (a behaviour change breaks the
   tutorial loudly, as H3 would have);
3. the tutor's intent for the step.

## The interactive layer

**Seeing commands.** Every CLI invocation already writes
`.remake/remake.jsonl` (`invocation` with argv, `plan`, `task_complete`,
`task_failed`, `run_summary`). Add an `invocation_end` event (exit code,
duration) so the tutor can tell "finished and failed" from "still
running". The tutor watches with a filtered `tail -F | jq` (Monitor tool),
one wake-up per command, not per task event. Token cost is small.

**Seeing edits — git.** The tutorial workspace is a git repo; each step's
reference state is a tag (`step-N`). On each command the tutor snapshots
the learner's tree with `git stash create` (no effect on their tree, index
or history) and diffs it against the step tag and against its previous
snapshot — so it can say "you only changed a comment, which is why nothing
reran". Plain shell commands (`ls`, `rm`) are invisible except through
their effects at the next `remake` command.

**Off piste.** The spec gives the step's *intent*; the *ground truth* is the
learner's actual state, read with remake itself (`run -n`, `why`) — never a
script match. Three cases:

- *Equivalent* (renamed things, other paths): accept; feedback on what
  actually happened.
- *Exploring* (extra commands, own experiments): answer, explain, then
  point back to the step.
- *Lost / broken* (mangled file, deleted `.remake/`): offer to restore —
  stash their work (never discard), check out `step-N`, replay the step's
  setup commands to rebuild outputs and DB.

**Tutor conduct** (in the skill): ask for a prediction before explaining;
stay quiet when things go as expected; go deep when the learner is
surprised.

**Docs stand alone** (mostly): every page works without Claude; the tutor
adds feedback, not content.

## Prototype

Lessons 0–2 + `invocation_end` + the `/remake-tutor` skill + workspace setup
(copy + `git init` + step tags). MM works through it in a second terminal
with the tutor watching; the loop's value is judged before lessons 3–8 are
written.

## Open

- Where the workspace setup lives: a `remake tutorial init <dir>` command
  (ships with the package) vs. a script in `examples/tutorial/`.
- Spec format and how the docs pages are generated from it (mkdocs macro
  vs. a checked-in build step).
