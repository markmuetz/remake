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
  - singleproc done; SLURM rewritten to the design (see
    [slurm_implementation.md](slurm_implementation.md)) and tested locally
    against fake sbatch/squeue shims — cluster validation on JASMIN pending;
    multiproc pending adaptation (not importable); dask deleted, re-add later
- [ ] Dynamic matrices (replanning loop, SLURM continuation jobs)
  - local replanning loop done; SLURM continuation jobs generated and
    tested locally; cluster validation pending
- [ ] CLI
  - Decision: argparse (stdlib, zero deps; CLI is small and the declarative
    wrapper in `util/command_line_args.py` already exists), not click.
    Done: run (--executor slurm, --dry-run), run-task, run-array-task,
    resubmit, info (--show-failures), version. migrate is its own item.
- [x] Unit tests
- [x] Integration tests
  - 77 tests under tests/unit and tests/integration; examples are loaded
    and planned as part of the suite. Grow alongside remaining items
    (multiproc/SLURM, CLI additions).
- [x] Runnable examples
  - All seven run end-to-end (synthetic inputs via
    examples/make_example_data.py); ex1/ex3/ex5 run in the test suite,
    heavier ones (xarray/zarr) verified manually and plan-tested.
- [ ] Documentation + deployment (GitHub Pages)
- [ ] Create PyPI package (verify build/install, settle PEP 440 version,
  first alpha upload)
- [ ] GitHub Actions: CI, docs, PyPI release
- [ ] Claude Code remake skill (`.claude/skills/remake/`) — see
  [claude_remake_skill.md](claude_remake_skill.md)
  - skeleton + authoring + migration + triage/monitoring/status sections
    done 2026-06-12; remaining: validate in anger on JASMIN, CLI gap
    items that earn their keep, health-check section, plugin packaging
- [ ] ~~remake2 migration tool~~ replaced by the migration section of the
  Claude skill (decision: LLM translation, not an automated tool)

See also: [todos.md](todos.md) (known problems and debts) and
[discussion.md](discussion.md) (ideas to return to).
