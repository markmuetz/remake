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
    Commands: run (-E singleproc/multiproc/slurm, -j, --dry-run, -X
    debugger on failure, exit 1 on task failure), run-task,
    run-array-task, resubmit, info (-t/-F/--json), ls-tasks, task-info,
    task-log, why, slurm-status, lint, version. Queries select by kwargs
    and rule name (`-Q "rule in ['r1', 'r2'] and year == 2010"`). Logs to
    stderr, data to stdout. Grew out of the gap audit in
    [claude_remake_skill.md](claude_remake_skill.md).
- [x] Unit tests
- [x] Integration tests
  - 123 tests under tests/unit and tests/integration; examples are loaded
    and planned as part of the suite, SLURM tested against fake
    sbatch/squeue shims. Grow alongside remaining items.
- [ ] Code coverage
  - pytest-cov + a threshold, wired into CI when GitHub Actions lands;
    use it to find untested corners (tokens, util) rather than chase 100%.
- [x] Runnable examples
  - All seven run end-to-end (synthetic inputs via
    examples/make_example_data.py); ex1/ex3/ex5 run in the test suite,
    heavier ones (xarray/zarr) verified manually and plan-tested.
- [ ] Documentation + deployment (GitHub Pages)
  - docs/ was deleted to start fresh; needs its own plan doc (tooling
    choice, structure). The design docs + examples + skill references are
    the source material.
- [ ] Create PyPI package
  - build/install/version verified locally (`uv build`, 0.8.0a0).
    Remaining: metadata polish (classifiers still say 3.9/Beta; check
    README renders as the long description), TestPyPI dry run, first
    alpha upload.
- [ ] GitHub Actions: CI, docs, PyPI release
  - CI = pytest (+ coverage above) on a small Python matrix; later the
    1e6-task benchmark as a load-bearing job (todos.md), docs deploy and
    a tag-triggered PyPI release.
- [ ] Claude Code remake skill (`.claude/skills/remake/`) — see
  [claude_remake_skill.md](claude_remake_skill.md)
  - skeleton + authoring + migration + triage/monitoring/status sections
    done and CLI gap items implemented 2026-06-12; remaining: validate in
    anger on JASMIN, health-check section, plugin packaging
- [x] ~~remake2 migration tool~~ replaced by the migration section of the
  Claude skill (decision: LLM translation, not an automated tool)

## What's left, at a glance (2026-06-12)

1. **JASMIN revalidation** of the post-livelock stack: sidecar/ingest at
   400/800-way through the real pipeline path, plus first real-cluster
   exercise of multiproc-on-a-login-node, slurm-status/why/lint and the
   skill (slurm_implementation.md §Suggested order item 4, second pass).
2. **Docs** — plan, write, deploy to GitHub Pages.
3. **GitHub Actions** — CI + coverage first; docs/release jobs after.
4. **PyPI** — metadata polish, TestPyPI, alpha upload.
5. Backlog beyond the plan: open items in [todos.md](todos.md)
   (benchmark in CI, batched completion transactions, fallback-mode stat
   cost, eval-query parser, uses-shadowing warning, Hypothesis tests,
   zarr v3 tokens, per-task log file-count budget) and the
   [discussion.md](discussion.md) ideas (terminal output, SLURM monitor,
   web interface, dask design, config cascade, logging injection,
   intra-rule deps, RO-Crate, plugins, .remake layout).

See also: [todos.md](todos.md) (known problems and debts) and
[discussion.md](discussion.md) (ideas to return to).
