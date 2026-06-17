# Claude Code remake skill

A Claude Code skill that makes Claude an effective remake operator and
author: diagnosing failures, monitoring SLURM runs, explaining rerun
decisions, writing and migrating pipelines. Lives in the repo at
`.claude/skills/remake/` so it ships with a clone (graduating to a
published plugin alongside the package if it proves out).

## Principles

1. **CLI-first, deliberately.** The skill's workflows are expressed as
   `remake` CLI invocations, not as reads of `.remake/` internals. This is
   a forcing function: anywhere the skill needs to poke a file or the DB
   directly, that is a CLI weakness — log it (todos.md / discussion.md)
   and, where it earns its keep, grow the CLI. Direct `.remake/` access is
   a documented last resort, read-only, and always paired with a logged
   gap. Two benefits: the skill keeps working when internals change, and
   the CLI improvements help human users and shell scripts equally.
2. **One skill, sectioned routing — not many skills.** A single
   `SKILL.md` with a routing preamble ("failure? → triage; writing rules?
   → authoring; old remakefile? → migration") plus shared context (the
   CLI surface, the `.remake/` map at the level a user would know it).
   Bulky reference material (the migration guide, the authoring contract)
   goes in `references/` files loaded only when that section is active.
3. **Encode observed behaviour, not guesses.** Triage/monitoring sections
   are written from real JASMIN failure modes (e.g. the 2026-06-12 log
   corruption, OOM/TIMEOUT kills) as they accumulate — not invented
   upfront.

## Sections (subskills)

### Authoring — write/modify rules correctly

The conventions a model gets subtly wrong without the spec in context:

- the signature contract: `def fn([inputs,] [outputs,] <matrix keys>)`,
  in that order; inputs/outputs/matrix all optional;
- scope rules: no free module globals — declare via `uses=`, environment
  objects (stdlib, modules) exempt; `strict_scope` tri-state;
- matrix forms (dict cartesian / list[dict] / `@deferrable` callable +
  `Defer`), `depends_on` is explicit, fan-in via input callables;
- `config={'slurm': {...}}` keys and merging; JSON-serialisable kwargs;
- pointers at `examples/ex1–ex9` as few-shot examples, and
  `remake run <file> --dry-run` / `info` as the check-your-work loop.

### Failure triage

`info --show-failures` → stored traceback → per-task log → SLURM
`.out`/`.err` → `sacct`. Classify before fixing: code bug vs environment
(import errors in the job env) vs resource kill (OOM/TIMEOUT — visible in
sacct, not tracebacks) vs missing-input cascade from an upstream failure.
Each class has a different fix path (edit rule / fix env / raise
mem-time / fix upstream first).

### SLURM monitoring

Per-rule progress (success/failed/pending/queued), mapping SLURM state
back to tasks, and the pathological states: `DependencyNeverSatisfied`
elements after partial upstream failure, stuck continuation jobs, arrays
pending on a missing partition/qos/account. Overlaps the "SLURM monitor"
discussion.md item — the skill is the cheap first version of it.

### Rerun explanation

"Why did/didn't task X rerun?" — walk the planner's actual logic in
order: DB status → run-code comparison → `uses_hash` → upstream
propagation (element-wise vs conservative) → `check_outputs` mode.
Mechanical, given the map; currently requires knowing internals (a known
CLI gap, below).

### Task status

Close to rerun explanation but answers the present, not the future:
"what state is task X / rule Y in right now?" — last run status and
timestamp, stored exception if failed, whether declared outputs exist on
disk, and whether a SLURM job for it is queued/running. Today this is
assembled from `info`/`info --tasks`/`info --show-failures` plus squeue;
there is no single-task detail view (a CLI gap, below). Distinct from
rerun explanation: status reports what *is*, rerun explanation reports
what the planner *would do* and why.

### Query crafting

`-Q` predicate syntax, missing-name-means-no-match semantics, safe use of
`--force`, dry-run habits.

### remake2 → remake3 migration

**Decision: migration is LLM translation, not an automated tool.** This
section replaces the "remake2 migration tool" implementation-plan item.
A syntax-transforming script would have to get semantics right
mechanically; an LLM with a good difference guide handles the long tail
(odd class hierarchies, helper methods, implicit globals) far better.

`references/remake2_to_remake3.md` carries the difference guide:

- **Rules**: `TaskRule` subclasses with `rule_inputs`/`rule_outputs`
  class attrs and a `run()` method → module-level `@rule` decorator on a
  plain function; `var_matrix` → `matrix`; the function signature must
  follow the remake3 contract.
- **Registration**: remake2's implicit registration at class definition →
  explicit `rmk = Remake(...)` at the top, `rmk.rules_from_current_module()`
  at the end.
- **Scope limitation**: remake2 `run()` methods freely used module
  globals; remake3 rejects undeclared free variables — migrate them to
  `uses=` (values *and* helper functions; both participate in rerun
  hashing).
- **Dependencies**: remake2 inferred a task DAG from path-string matching;
  remake3 needs explicit `depends_on` and has no task-level DAG —
  intra-rule task dependencies must be restructured into separate rules.
- **SLURM**: per-task jobs → per-rule array jobs; config keys
  (`partition`/`qos` on current JASMIN, `array_threshold`,
  `array_throttle`); `remake run-tasks` → `run`/`run-task`/
  `run-array-task`/`resubmit`.
- **Filtering**: pyquerylist task lists → `-Q` eval predicates.
- **Metadata**: no DB migration — remake3 starts a fresh `.remake/`;
  first run re-establishes state (use `check_outputs='fallback'` so
  existing on-disk outputs are recognised without rerunning).
- Gone: `archive`, dask executor (for now), task-level prev/next pointers.

Workflow: read the remake2 file → translate → `remake run <file>
--dry-run` to validate (plan should reflect existing outputs) → run a
small `-Q` slice before the full pipeline.

### Health check (later)

"What state is this pipeline in": consistency sweep of plan vs DB vs
queued jobs. Mostly blocked on CLI gaps below; implement once those land.

## CLI gap audit and new commands (2026-06-12)

The audit question: what information is *only* available by examining
`.remake/` directly? Findings:

| Information | Lives in | CLI before audit |
|---|---|---|
| status counts, failures + tracebacks | `remake.db` | `info`, `-t`, `-F` |
| last-run timestamp per task | `remake.db` | — |
| stored run code / uses_hash (what changed) | `remake.db` | — |
| task key ↔ array index ↔ kwargs | `jobs/<rule>.json` | — |
| rule ↔ SLURM job id(s) | `jobs/<rule>.jobids.json` | — |
| per-task log location/content | `tasks/log/...` | — |
| pending un-ingested sidecars | `tasks/results/` | — (transient by design) |
| generated sbatch/submit.sh | `slurm/` | — (fine as plain files) |

Commands added to close the gaps (all task-addressing commands accept a
key prefix *or* `-Q query` resolving to exactly one task). The query
namespace is the task kwargs plus `rule` (the rule name), so queries
select rules too: `-Q "rule in ['r1', 'r2'] and year == 2010"` — this
replaced an earlier `-R <rule>` flag (one mechanism, composable):

- `remake ls-tasks <file> [-Q q] [--json]` — enumerate tasks
  (key + name), streaming in text mode; deferred rules are reported, not
  an error. Pure expansion, no DB reads — the cheap "what exists" view
  next to `info -t`'s "what state" view.
- `remake task-info <file> [key-prefix|-Q q] [--json]` — single-task
  detail: rule, kwargs, key, status + timestamp, inputs/outputs with
  on-disk existence, per-task log path, SLURM job id + array index from
  the last submission, traceback if failed.
- `remake task-log <file> [key-prefix|-Q q] [--path]` — print the
  per-task log (or just its path, for piping).
- `remake why <file> [key-prefix|-Q q]` — rerun explanation: walks the
  planner's checks for one task and reports what applies (never run /
  failed / run code changed, with diff / uses changed / outputs
  incomplete / upstream propagation, element-wise vs conservative).
- `remake slurm-status <file> [--json]` — per rule: last submitted job
  id(s) and live squeue element states/reasons (surfaces
  `DependencyNeverSatisfied` etc.).
- `remake lint <file> [--json]` — wiring validation (the discussion.md
  task-inspection idea): NEAR MISS (an input nothing produces, but an
  upstream produces something ≥0.9 similar — typo territory), MISSING
  DEPENDENCY (input produced by an undeclared rule), external (counted,
  informational). Near-miss search is restricted to declared upstreams'
  outputs, so cost stays sane and source-data inputs aren't fuzzily
  matched against the whole pipeline. Exit 1 on findings.
- `info --json` (also on `task-info`/`slurm-status`) — machine-readable
  output; the skill's workflows parse this instead of aligned text.
- Logger output moved to stderr so `--json` stdout is clean.

Deliberately not commands: sidecar visibility (transient; ingest logs a
line) and generated-script viewing (they are files meant for reading).

## Sequencing

1. Skill skeleton + authoring + migration sections (stable now: design
   docs and examples are the source material).
2. Triage + monitoring + query sections, written against the CLI as it
   stands; log gaps found.
3. CLI gap items that earned their keep; health check section once they
   land.
4. Consider plugin packaging once used in anger on JASMIN.
