# Attribution

## Provenance

remake3 is a clean-break rewrite of **remake** (remake2), authored and directed
by **Mark Muetzelfeldt** (markmuetz@gmail.com), the original author and designer
of remake. The rewrite did not start from a blank page: it **began from the
remake2 codebase** (`src/remake/`, preserved in git history and on the `remake2`
branch) and reworked it in place — keeping remake2's core concepts (rules,
matrices/tasks, a `.remake/` metadata store, stale-rebuild semantics, SLURM
execution) while replacing the API and internals.

The implementation, drafting and much of the option-generation in the rewrite
were carried out by Claude Code (Anthropic's CLI; Claude Opus models) working
under Mark's direction. **Every design decision recorded below was made or
approved by Mark.** This document records the design contributions that
originated with him, reconstructed from the session transcripts in
`~/.claude/projects/.../*.jsonl`.

## Overall direction

- Set the project's goal, scope and constraints throughout: a make-like build
  tool for file-based science pipelines on HPC/SLURM (esp. JASMIN), prioritising
  large task graphs / many files and stale-rebuild correctness, in pure Python.
- Chose the clean-break rewrite strategy, the `remake` package/CLI name with
  `remake3` as a working title, the `0.8.0a0` alpha versioning, the move to
  `src/` layout, `pyproject.toml`/`uv`, and `pytest`.
- Identified remake's **niche** and had it written into the project's
  positioning: *"for people with large task graphs and/or lots of files, who
  need SLURM and stale rebuild capabilities"* — informed by the `remake_vs`
  comparison against snakemake/luigi/Parsl that he commissioned.

## API and core model

- **`from remake import rule` decorator.** Argued that the decorator should be a
  plain import, not a method on a `Remake` instance, so rules can be defined
  across multiple files and combined in a top-level file. Asked for a
  `Remake.from_current_module()` / `rules_from_current_module()` factory to
  collect them.
- **Optional outputs.** Motivated by rules that set up or write to a zarr store
  or database and produce no tangible file — outputs should not be mandatory.
- **Transparent output tokens.** Pushed on whether tokens were needed at all,
  then settled the decision to keep them but make them transparent.
- **Automatic parent-directory creation** before a task runs (removing the
  boilerplate `Path(...).parent.mkdir(...)` from every rule).
- **Rule-level (not task-level) DAG** — accepted and documented the limitation.
- **`uses=` change detection** — raised the question of whether a function
  passed via `uses` should trigger a rerun when it changes, and later asked for
  detection of *which* `uses` key changed.
- **`depends_on` / inputs flexibility** — confirmed lambda/closure inputs should
  work and asked for it to be documented.
- **Per-rule config** — proposed a `config` dict on the `@rule` decorator for
  per-rule settings such as high-memory/CPU SLURM resources.
- **Matrix tuple-key form** — brought across from remake2: an N-tuple key with a
  list of N-value tuples (alongside the Cartesian-product form), for building a
  sequence then filtering on data availability.

## Execution, SLURM and metadata

- **Custom executor injection** — asked that users be able to subclass the base
  `Executor` and inject it.
- **Upstream-failure handling** — flagged that when an upstream task fails,
  downstream tasks must be skipped rather than run into missing inputs.
- **SLURM output file layout** — proposed sharding
  `.remake/slurm/output/<rule>/<id:0-2>/<id:2->` to avoid too many files in one
  directory (a real JASMIN/Lustre concern).
- **Sidecar/ingest model** and, later, the **fallback "halfway house"** —
  suggested tasks try to write `remake.db` directly and fall back to sidecars
  only on contention, to relieve DB pressure in the common case.
- **remake-daemon** orchestration concept (discussed and parked).
- **Opt-in stats/run-history record** — proposed recording run statistics, kept
  deliberately opt-in.

## CLI and tooling

- **Query syntax** — asked for `-Q` to target multiple rules, e.g.
  `-Q "rule in ['r1', 'r2']"`.
- **`--ignore-code-changes` / `-I`** and the **`set-state`** subcommand —
  defined the semantics: rerun only what has never *succeeded* (failed counts as
  not-run), with upstream propagation still triggering reruns; recalled
  `set-state` from remake2 for setting task state by query.
- **CLI inspection commands** — drove `task-info`, `task-log`, `ls-tasks`,
  `lint` (near-miss input wiring), and `rule-dag` (with `-N`/`-M` options and
  `?` for not-yet-known continuation-task counts), plus input/output listing
  with `--check`.
- **`--raise`** — asked for a flag to raise the first exception (singleproc).
- **`remake run -X`** debugger flag (moved from the old `remake -X run`).
- **Logging philosophy** — *debug summarises loops, trace lists each element in
  a loop.*

## The Claude Code remake skill

- Originated the idea of a Claude Code skill that diagnoses errors, suggests
  fixes, searches logs, monitors SLURM jobs, and advises on resource use
  (MaxRSS/wallclock lookback) — and insisted it be **CLI-first**, driving the
  design of the inspection commands above. Also set the direction that
  remake2→remake3 migration be done by an LLM rather than an automated tool.

## Correctness: bug 01 — durable rerun propagation

This bug and its fix are Mark's, end to end (see
`design_docs/bugs/01_durable_rerun_propagation.md`):

- Spotted the **missing planner step**: *"Is there not an extra step in the
  plan: if a previous task has a more recent run time than the current task, it
  needs rerun?"*
- Identified the **crash scenario**: *"what would happen … if the code change to
  A caused A to succeed, but the process dies … in between A succeeding and B
  running? A is marked as successfully run … but B is now also marked as
  successfully having run?"*
- Proposed the **solution direction** that was adopted: *"store task execution
  time in remake.db, not file mtime. This has the benefit that we are in control
  of it. You get the same benefits as make."* (implemented as a logical
  `run_seq` rather than a clock).
- Added the **partial-target failure case**: *"you just target a specific task
  to run with `-Q "rule == 'A'"`. There is no failure needed."*
- Raised the **A→B→C cascade consequence** for `set-state` and asked whether
  settling B should also update its descendants, and whether that should be
  default or optional.
- Proposed the **cascade guard**: *"if a task (D) has multiple dependencies, AND
  one of these has been rerun, do not restamp it."*

## Documentation and positioning

- Directed the docs setup (mkdocs + GitHub Pages), the CI/coverage/release
  workflows (Trusted Publishing to PyPI), the design-doc-based planning process,
  the `key concepts` and `task state` explainers, the basic→advanced ordering of
  the worked examples, and the `remake_vs` comparison study and niche write-up.

---

*Compiled by Claude Code from the project's session transcripts at Mark's
request. The framing throughout: Mark Muetzelfeldt is the designer and decision
-maker; Claude Code implemented under his direction, starting from the remake2
codebase.*
