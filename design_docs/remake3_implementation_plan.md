# remake3 — Implementation Plan

High-level progress tracking for implementing
[remake3_design.md](remake3_design.md). Each item will get its own design/plan
doc in `design_docs/` when work on it starts.

- [x] Initial decisions: package/import name, fate of remake2 code in
  `src/remake/`, versioning, CLI entry point name
  - remake3 is the project's working name only; the package, imports and CLI
    are all `remake`. remake3 code replaces `src/remake/` in place (remake2
    is preserved in git history / the remake2 branch). Version: 0.8.0a0.
- [x] Core: `@rule` decorator, `Rule`, `Task`, DAG, planner, scope analysis,
  `Remake` class
- [x] Output tokens
- [x] Metadata backend (SQLite)
- [x] Executors: singleproc, multiproc, SLURM (dask: deliberately dropped)
  - singleproc done; SLURM done and validated on JASMIN (ex2/ex4/ex8 on
    real SLURM, 2026-06-12; see
    [slurm_implementation.md](slurm_implementation.md)), with sidecar/
    ingest result recording after the SQLite livelock finding — remaining
    SLURM item is revalidating contention at 400/800-way through the real
    pipeline path; multiproc rewritten 2026-06-12 (spawned workers reload
    the remakefile, sidecar results, per-rule barriers; `-E multiproc -j N`);
    dask is not part of this item — it needs its own design (see
    discussion.md) and re-adds against the Executor ABC when its turn comes
- [x] Dynamic matrices (replanning loop, SLURM continuation jobs)
  - local replanning loop and SLURM continuation jobs done; ex8-style
    continuation chains validated on real SLURM (JASMIN, 2026-06-12)
- [x] CLI
  - Decision: argparse (stdlib, zero deps; CLI is small and the declarative
    wrapper in `util/command_line_args.py` already exists), not click.
    Commands: run (-E slurm, --dry-run, exit 1 on task failure), run-task,
    run-array-task, resubmit, info (-t/-F/--json), ls-tasks, task-info,
    task-log, why, slurm-status, lint, version. Logs to stderr, data to
    stdout. Grew out of the gap audit in
    [claude_remake_skill.md](claude_remake_skill.md).
- [x] Unit tests
- [x] Integration tests
  - 115 tests under tests/unit and tests/integration; examples are loaded
    and planned as part of the suite, SLURM tested against fake
    sbatch/squeue shims. Grow alongside remaining items.
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
- [x] ~~remake2 migration tool~~ replaced by the migration section of the
  Claude skill (decision: LLM translation, not an automated tool)

See also: [todos.md](todos.md) (known problems and debts) and
[discussion.md](discussion.md) (ideas to return to).
