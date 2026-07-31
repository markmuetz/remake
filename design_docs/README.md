# design_docs index

Engineering docs for remake3. Not user documentation (that's `docs/`, the
MkDocs site) — this is where designs are argued, decisions recorded, and
debts tracked. **Not versioned separately**: git history + release tags are
the versioning; instead each doc has a *class* that says how its truth is
maintained. When adding a doc, say which class it is.

- **Living** — normative; must be kept true as the code moves.
- **Working** — open state; items move out when resolved (to an archive
  file, a bug doc, or a Graduated record).
- **Record** — an execution/analysis snapshot; frozen with a status banner,
  point-in-time details are not maintained ("trust the code").

## Living

- [remake3_design.md](remake3_design.md) — the design document: motivation,
  principles, scale target, API, architecture, schema, SLURM/dynamic
  matrices/tokens, out-of-scope list.
- [compatibility.md](compatibility.md) — backwards-compatibility policy:
  three surfaces, pre-1.0 conduct, the 1.0 contract.
- [roadmap.md](roadmap.md) — positioning and milestones beyond 0.8.0
  (0.9.x → 1.0), cross-cutting scale-debt, explicit non-goals.
- [alternatives.md](alternatives.md) — how remake relates to snakemake/
  luigi/orchestrators; the niche it optimises for.

## Working

- [todos.md](todos.md) — concrete known problems and debts, checkbox state.
  At a release, prune `[x]` entries to
  [todos_archive.md](todos_archive.md) so the live list stays readable
  (last prune: 2026-07-03, at 0.8.0).
- [discussion.md](discussion.md) — ideas to return to; not commitments.
  Implemented items graduate to
  [graduated_discussion.md](graduated_discussion.md); confirmed bugs move
  to `bugs/`.
- [bugs/](bugs/) — one numbered file per confirmed bug, status in the
  header (01 durable propagation — fixed; 02 task-info non-path input —
  open; 03 fresh-DB adoption trap — open, premise partly overtaken;
  04 info double-query/superlinear status — fixed; 05 SLURM sidecar
  run-code stamping — fixed).
- [future_releases/](future_releases/) — scoped feature slices for
  upcoming milestones (v0.9.0.md).
- [MM_review.md](MM_review.md) — running log of Mark's source reviews
  (`# MM:` comments) and their outcomes.
## Records

- [remake3_0.8.0_release.md](remake3_0.8.0_release.md) — the road to the
  0.8.0 release (blocking items, DoD checklist, branch migration), frozen
  at the v0.8.0 tag as the release's execution record.

- [graduated_discussion.md](graduated_discussion.md) — discussion items
  that shipped, kept with their full design reasoning and implementation
  postscripts (incl. the 2026-07 `uses`/`io` storage rework), plus settled
  design decisions (rule syntax: decorator over class).
- [todos_archive.md](todos_archive.md) — completed todos pruned from the
  live list at each release, verbatim.
- [remake3_implementation_plan.md](remake3_implementation_plan.md) — the
  build-out progress tracker (complete; counts are point-in-time).
- [detailed_code_implementation.md](detailed_code_implementation.md) — the
  remake2→remake3 transformation plan (executed; known divergences noted).
- [slurm_implementation.md](slurm_implementation.md) — SLURM executor
  implementation and JASMIN validation record (incl. the SQLite livelock
  finding and the sidecar design).
- [per_task_logging.md](per_task_logging.md) — per-task log design (shared
  log corruption fix) and the log-level convention.
- [slurm_already_running.md](slurm_already_running.md) — design for
  per-task already-running detection (open todo; design ready).
- [rocrate_export.md](rocrate_export.md) — RO-Crate provenance export
  design (scheduled 0.10.x).
- [resource_capture.md](resource_capture.md) — per-task wall/CPU/peak-RSS
  capture design (0.9.0 item 1).
- [dir_outputs.md](dir_outputs.md) — directory outputs (`Dir` token) design
  (0.9.0 item 6).
- [remakefile_deps.md](remakefile_deps.md) — cross-remakefile dependencies
  (PARKED) and the `run-all` discovery half (0.9.0 item 7).
- [claude_remake_skill.md](claude_remake_skill.md) — plan for the Claude
  Code remake skill and the CLI gap audit it drove.
- [attribution.md](attribution.md) — who contributed which design ideas,
  reconstructed from session transcripts.
- [jasmin_remake_dirs.md](jasmin_remake_dirs.md) — survey of the `.remake/`
  dirs on JASMIN by remake version (2026-07-02 snapshot).
- [logs_analysis/](logs_analysis/) — field `remake.log` mining (2026-07-02):
  the status-query amplification finding + timing CSVs; drove the storage
  rework and the logging split.
- [MM_review.md](MM_review.md) is listed under Working (it accretes) but
  individual review outcomes are records.
