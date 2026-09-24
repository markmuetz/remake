# design_docs index

Engineering docs for remake. Not user documentation (that's `docs/`, the
MkDocs site) — this is where designs are argued, decisions recorded, and
debts tracked. **Not versioned separately**: git history + release tags are
the versioning; instead each doc has a *class* that says how its truth is
maintained, and the folder it lives in says which class it is.

| Class | Where | How its truth is kept |
|---|---|---|
| **Living** | top level | Normative; kept true as the code moves. |
| **Working** | top level, `bugs/` | Open state; items move out when resolved. |
| **Design** | `designs/` | One feature per doc, with a status banner (Proposed / Agreed / Implemented / Parked). Decisions are settled unless marked open. |
| **Release** | `releases/` | The scope of an upcoming minor release; frozen as a record once tagged. |
| **Record** | `records/`, `code_reviews/` | A dated snapshot, frozen with a banner; point-in-time details are not maintained ("trust the code"). |

Restructured 2026-09-24 (folders by class; `remake3_` prefixes dropped).

## How an idea moves through these docs

1. **Raised** → an item in [discussion.md](discussion.md) (not a commitment).
2. **Designed** → its own doc in [designs/](designs/); discussion.md keeps a
   one-line pointer.
3. **Scheduled** → a milestone in [roadmap.md](roadmap.md), and scoped in
   the release doc in [releases/](releases/).
4. **Shipped** → the discussion text moves verbatim to
   [records/graduated_discussion.md](records/graduated_discussion.md); the
   design doc's banner says *Implemented*.

Problems take a parallel path: a known debt is a checkbox in
[todos.md](todos.md) (pruned to
[records/todos_archive.md](records/todos_archive.md) at each release); a
confirmed bug with a real analysis gets a numbered file in [bugs/](bugs/);
review findings live in [code_reviews/](code_reviews/) and are tracked in
todos.md by review ID once scheduled.

## Living

- [design.md](design.md) — the design document: motivation, principles,
  scale target, API, architecture, schema, SLURM/dynamic matrices/tokens,
  out-of-scope list. (Known divergences from the code are bannered at the
  top; a full accuracy pass is owed.)
- [roadmap.md](roadmap.md) — the plan to 1.0 and beyond: positioning, the
  "does 1.0 need it?" test, milestones (0.8.x lane, 0.9, 0.10, 1.0), the
  after-1.0 backlog, non-goals, and the 2026-09-24 assessment of the
  previous plan.
- [compatibility.md](compatibility.md) — backwards-compatibility policy:
  three surfaces, patch-lane rules, pre-1.0 conduct, the 1.0 contract.
- [alternatives.md](alternatives.md) — how remake relates to Snakemake,
  luigi, orchestrators; the niche it optimises for.

## Working

- [todos.md](todos.md) — concrete known problems and debts, checkbox
  state; the 0.8.4 patch list by review ID.
- [discussion.md](discussion.md) — ideas by theme, with an index of the
  ones the roadmap schedules, plus Parked and Rejected sections.
- [MM_review.md](MM_review.md) — running log of Mark's source reviews
  (`# MM:` comments) and their outcomes.
- [bugs/](bugs/) — one numbered file per confirmed bug, status in its
  header: 01 durable propagation (fixed; multiproc/dask regression noted),
  02 task-info on non-path input (open), 03 fresh-DB adoption (closed —
  superseded), 04 info status queries (fixed), 05 SLURM sidecar run-code
  (fixed).

## Designs — `designs/`

| Doc | Status | Milestone |
|---|---|---|
| [resource_capture.md](designs/resource_capture.md) | Implemented (pre-tag review owed) | 0.9 |
| [dir_outputs.md](designs/dir_outputs.md) — `Dir` token | Agreed | 0.9 |
| [remakefile_deps.md](designs/remakefile_deps.md) — `run-all` + cross-remakefile deps | `run-all` agreed; deps parked | 0.9 (`run-all`) |
| [rocrate_export.md](designs/rocrate_export.md) — RO-Crate export | Design | after 1.0 |
| [slurm_already_running.md](designs/slurm_already_running.md) — duplicate-submission guard | Implemented in part; rest parked | — |
| [per_task_logging.md](designs/per_task_logging.md) — per-task log layout | Implemented | — |
| [claude_remake_skill.md](designs/claude_remake_skill.md) — the Claude Code remake skill | Implemented | — |

The H3 fix design (path-derived task dependencies) currently lives in the
[2026-09-24 review](code_reviews/2026-09-24_review.md), Appendix A; it moves
here when work starts.

## Releases — `releases/`

- [v0.9.0.md](releases/v0.9.0.md) — scope of the next minor: correctness of
  the core + DSL shape (rescoped 2026-09-24).
- [v0.8.0_record.md](releases/v0.8.0_record.md) — the road to 0.8.0
  (blocking items, DoD checklist, branch migration); frozen at the tag.

## Records — `records/` and `code_reviews/`

- [code_reviews/](code_reviews/) —
  [2026-07-09](code_reviews/2026-07-09_review.md) (SLURM submission logic,
  fixed in 0.8.1) and [2026-09-24](code_reviews/2026-09-24_review.md) (full
  implementation excl. SLURM: bugs, API/CLI rough edges, missing features,
  H3 fix design).
- [graduated_discussion.md](records/graduated_discussion.md) — discussion
  items that shipped, with their design reasoning and postscripts, plus
  settled design decisions (e.g. decorator over class).
- [todos_archive.md](records/todos_archive.md) — completed todos pruned at
  each release, verbatim.
- [implementation_plan.md](records/implementation_plan.md) — the remake3
  build-out tracker (complete; counts point-in-time).
- [detailed_code_implementation.md](records/detailed_code_implementation.md)
  — the remake2→remake3 transformation plan (executed; divergences noted).
- [slurm_implementation.md](records/slurm_implementation.md) — SLURM
  executor build-out and JASMIN validation (SQLite livelock finding, sidecar
  design).
- [logs_analysis/](records/logs_analysis/) — field `remake.log` mining
  (2026-07-02): the status-query amplification finding + timing CSVs.
- [jasmin_remake_dirs.md](records/jasmin_remake_dirs.md) — survey of
  `.remake/` dirs on JASMIN by remake version (2026-07-02).
- [attribution.md](records/attribution.md) — who contributed which design
  ideas, reconstructed from session transcripts.
