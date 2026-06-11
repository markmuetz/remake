# remake3 — Implementation Plan

High-level progress tracking for implementing
[remake3_design.md](remake3_design.md). Each item will get its own design/plan
doc in `design_docs/` when work on it starts.

- [ ] Initial decisions: package/import name, fate of remake2 code in
  `src/remake/`, versioning, CLI entry point name
- [ ] Core: `@rule` decorator, `Rule`, `Task`, DAG, planner, scope analysis,
  `Remake` class
- [ ] Output tokens
- [ ] Metadata backend (SQLite)
- [ ] Executors: singleproc, multiproc, SLURM, dask
- [ ] Dynamic matrices (replanning loop, SLURM continuation jobs)
- [ ] CLI
- [ ] Unit tests
- [ ] Integration tests
- [ ] Runnable examples
- [ ] Documentation + deployment (GitHub Pages)
- [ ] GitHub Actions: CI, docs, PyPI release
- [ ] remake2 migration tool
