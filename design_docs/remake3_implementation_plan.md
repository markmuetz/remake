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
- [x] Executors: singleproc, multiproc, SLURM, dask
  - singleproc done; SLURM done and validated on JASMIN (ex2/ex4/ex8 on
    real SLURM, 2026-06-12; see
    [slurm_implementation.md](slurm_implementation.md)), with sidecar/
    ingest result recording after the SQLite livelock finding; contention
    revalidated at 400/800-way through the real pipeline path on JASMIN
    (2026-06-13, bench_slurm_pipeline.py — both PASS, no lock errors,
    ingest ~2.5 ms/sidecar linear); multiproc rewritten 2026-06-12 (spawned workers reload
    the remakefile, sidecar results, per-rule barriers; `-E multiproc -j N`);
    dask added 2026-06-12 on the same model (LocalCluster by default,
    `config={'dask': {'scheduler': ...}}` for real clusters; `distributed`
    is the `remake[dask]` extra) — deeper dask-native integration
    (inter-rule futures, dask-jobqueue) stays a discussion.md item
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
- [x] Code coverage
  - pytest-cov wired into CI (2026-06-15): `--cov=remake` with
    term-missing + xml reports, `fail_under=85` (currently ~89%). Use the
    term-missing output to find untested corners (util/config, dask/
    multiproc executor branches) rather than chase 100%.
- [x] Runnable examples
  - All seven run end-to-end (synthetic inputs via
    examples/make_example_data.py); ex1/ex3/ex5 run in the test suite,
    heavier ones (xarray/zarr) verified manually and plan-tested.
- [x] Documentation + GitHub Pages deployment
  - MkDocs site (Material + mkdocstrings) under docs/, built from README,
    examples/, remake_cmd.py and the design/skill references: home,
    installation, getting-started, user guide (rules/tasks incl. all four
    matrix forms + dynamic matrices, running, SLURM, debugging), CLI
    reference, mkdocstrings API reference. `docs` dependency group added;
    `mkdocs build --strict` passes (`uv run mkdocs serve` to preview).
    Merged to remake3 2026-06-15. Deployed to GitHub Pages
    (https://markmuetz.github.io/remake) via .github/workflows/docs.yml
    on push to remake3 (Pages source switched from legacy main/docs to
    the Actions workflow, 2026-06-15). Remaining: a docs-vs-code accuracy
    pass as the API settles.
- [x] Create PyPI package — 0.8.0a0 published 2026-06-15
  - metadata polished (Development Status -> 3 - Alpha, Apache license +
    Topic classifiers, Python 3.10–3.14, README as markdown long
    description); `twine check` clean. Published to PyPI via release.yml
    Trusted Publishing on the v0.8.0a0 tag (TestPyPI dry run skipped —
    login trouble; went straight to PyPI with a required-reviewer gate on
    the `pypi` environment as the safety net). First publish attempt failed
    on just-registered-publisher timing; rerun succeeded. Verified
    `pip install remake==0.8.0a0` (pre-release, so not the default install).
- [~] GitHub Actions: CI, docs, PyPI release
  - CI done (.github/workflows/ci.yml, 2026-06-15): pytest on a Python
    3.10–3.14 matrix via uv (`uv sync --group dev` + `uv run pytest`), on
    push to remake3/remake2/main and all PRs; SLURM tests run against the
    fake sbatch/squeue shims so no cluster is needed. Coverage wired in
    too (see above). Remaining: the 1e6-task benchmark as a load-bearing
    job (todos.md), a docs deploy job (GitHub Pages) and a tag-triggered
    PyPI release.
- [ ] Claude Code remake skill (`.claude/skills/remake/`) — see
  [claude_remake_skill.md](claude_remake_skill.md)
  - skeleton + authoring + migration + triage/monitoring/status sections
    done and CLI gap items implemented 2026-06-12; remaining: validate in
    anger on JASMIN, health-check section, plugin packaging
- [x] ~~remake2 migration tool~~ replaced by the migration section of the
  Claude skill (decision: LLM translation, not an automated tool)

## What's left, at a glance (2026-06-12)

The road from the shipped `0.8.0a0` alpha to the full `0.8.0` release —
including the `remake3`→`main` branch migration — is laid out in
[remake3_0.8.0_release.md](remake3_0.8.0_release.md).


1. **JASMIN revalidation** of the post-livelock stack:
   - ~~sidecar/ingest at 400/800-way through the real pipeline path~~ —
     done 2026-06-13 (bench_slurm_pipeline.py; both PASS, no lock errors,
     ingest ~2.5 ms/sidecar linear; slurm_implementation.md §Suggested
     order item 4, second pass).
   - ~~first real-cluster exercise of multiproc-on-a-login-node~~ — done
     2026-06-15 on a 48-core JASMIN sci node (`host838`, not in a SLURM
     allocation). 64-task two-rule pipeline, 8 deliberate stage1 failures,
     `-E multiproc -j 8`: 6.3 s wall vs a 32 s serial floor (~6–8× on 8
     workers); `upstream_failed` skip correct (stage1 56/8/0, stage2
     56/0/8 — the 8 dependants left pending, not run-into-missing-inputs);
     per-task logs landed under `.remake/tasks/log/...` (120 files; skipped
     tasks produce none); all worker sidecars ingested (0 left). Capped at
     8 workers deliberately (shared node) — the concurrency-critical
     sidecar path was already proven harder at 800-way under SLURM, so a
     high-core run adds nothing for correctness. Follow-up made the same
     day: `MultiprocExecutor` nproc default now `os.sched_getaffinity`
     (respects the cpuset/SLURM `--cpus-per-task` mask) instead of
     `os.cpu_count()`, which would oversubscribe inside an allocation on a
     big node.
   - ~~slurm-status/why/lint~~ — validated on JASMIN 2026-06-15 against a
     live SLURM submission (12-task two-rule pipeline, `array_throttle=6`,
     stage1 sleeping so the array stayed in R): `lint` reported "all inputs
     wired to declared dependencies"; `why` on an unrun task gave "will run:
     yes — never run (no DB record)"; `slurm-status` (human + `--json`) read
     the queue accurately — `stage1 PD:6 R:6 [JobArrayTaskLimit]` (exactly
     the %6 throttle), `stage2 PD:12 [Dependency]`, and the drained-queue
     fallback ("not in queue") after scancel. Minor UX nit found: a `why`
     `-Q` query matching >1 task raises `RemakeError` as an uncaught
     traceback rather than a clean message (logged for the CLI-polish pass).
   - Remaining: the skill.
2. **Docs** — ~~plan, write, deploy~~ done; live at
   https://markmuetz.github.io/remake (Pages via Actions, 2026-06-15).
3. **GitHub Actions** — ~~CI, coverage, docs-deploy, release job~~ done
   (pytest matrix + pytest-cov + Pages deploy + Trusted-Publishing release,
   2026-06-15). release.yml: workflow_dispatch -> TestPyPI dry run, v* tag
   -> PyPI.
4. **PyPI** — ~~metadata polish, alpha upload~~ done: 0.8.0a0 live on PyPI
   (2026-06-15, Trusted Publishing). TestPyPI dry run skipped.
5. Backlog beyond the plan: open items in [todos.md](todos.md)
   (benchmark in CI, batched completion transactions, fallback-mode stat
   cost, eval-query parser, uses-shadowing warning, Hypothesis tests,
   zarr v3 tokens, per-task log file-count budget) and the
   [discussion.md](discussion.md) ideas (terminal output, SLURM monitor,
   web interface, dask design, config cascade, logging injection,
   intra-rule deps, RO-Crate, plugins, .remake layout).

See also: [todos.md](todos.md) (known problems and debts) and
[discussion.md](discussion.md) (ideas to return to).
