# remake3 — Implementation Plan

High-level progress tracking for implementing
[remake3_design.md](remake3_design.md). Each item will get its own design/plan
doc in `design_docs/` when work on it starts.

- [x] Initial decisions: package/import name, fate of remake2 code in
  `src/remake/`, versioning, CLI entry point name
  - remake3 is the project's working name only; the package, imports and CLI
    are all `remake`. remake3 code replaces `src/remake/` in place (remake2
    is preserved in git history / the remake2 branch). Version: 0.8.0.0-alpha.
- [x] Core: `@rule` decorator, `Rule`, `Task`, DAG, planner, scope analysis,
  `Remake` class
- [x] Output tokens
- [x] Metadata backend (SQLite)
- [ ] Executors: singleproc, multiproc, SLURM, dask
  - singleproc done; multiproc/slurm pending adaptation (not importable);
    dask deleted, re-add later
- [ ] Dynamic matrices (replanning loop, SLURM continuation jobs)
  - local replanning loop done; SLURM continuation jobs pending
- [ ] CLI
- [ ] Unit tests
- [ ] Integration tests
- [ ] Runnable examples
- [ ] Documentation + deployment (GitHub Pages)
- [ ] GitHub Actions: CI, docs, PyPI release
- [ ] remake2 migration tool
