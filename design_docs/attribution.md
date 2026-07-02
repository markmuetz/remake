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
- **`uses`/`io` code storage rework.** Directed the design (recorded in
  `discussion.md`) that moves `uses`/`io` source out of the verbose per-task
  `uses_hash`/`io_hash` columns and into the content-addressed `code` table at
  rule granularity, demoting the per-task column to a genuine digest — unlocking
  human-readable `uses` code-change diffs and `rule-info` source display while
  shrinking per-task rows at scale. Critically, **caught the design gap** that
  `uses` is N heterogeneous helpers (some sourceable functions, some plain
  values, some sourceless callables) that cannot map to a single FK the way
  run/inputs/outputs do — forcing the `rule_uses(rule_id, name, code_id, kind)`
  join table with a per-entry `kind` marker so only diffable source is rendered
  as such.
- **Rule provenance + duplicate-rule-name guard.** From discussing the single
  `.remake/` store shared by co-located remakefiles, identified the silent
  collision when two remakefiles define a different rule under the same name
  (they clobber each other's recorded state, logs and SLURM job specs) and
  directed the fix: record the owning remakefile in the `rule` table and warn
  when a same-named rule's code differs and was last written by another file.
  Framed the broader rationale — *"it is good for the DB to know where rules
  are defined anyway"* — so provenance also underpins inspection and future GC
  of orphaned records.

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

## Field validation on JASMIN (added on the HPC machine)

The contributions below were reconstructed from the session transcripts on the
**JASMIN machine** — the sessions where remake3 first met real SLURM, real
shared NFS, and real remake2 pipelines. They are Mark's, made or approved by him
under the same framing as above. Where they touch areas already listed (the
sidecar model, SLURM layout, the skill), they record the empirical pressure that
shaped those decisions in practice rather than on paper.

### SQLite contention — empirical scepticism

- **Refused to declare the lock-retry machinery proven on one data point.** After
  a 176-element `ex4` array hit `.remake/remake.db` on NFS with zero lock errors,
  rejected marking `retry_lock_commit` resolved: *"I don't think we've stressed
  `.remake/remake.db` enough yet. Let's write a custom script to really hammer it
  from a bunch of SLURM array jobs."* Directed a dedicated stress harness
  (`tests/benchmarks/bench_sqlite_contention.py`) of independent, output-less,
  near-instant rules with no `depends_on`, so all array jobs hammer the DB
  concurrently with no staggering.
- **Drove the bisection toward the contention cliff** (800-way → 400-way) and
  recognised the harness was pushing the DB far harder than any real run so far —
  surfacing a livelock well below 400-way concurrency that the moderate `ex4`
  width had hidden.
- **Single-source-of-truth critique of the sidecar/serial-ingest fix.** Flagged
  the cost that the proposed contention relief introduces: *"there's no longer a
  single source of truth for the state of the tasks. Any `remake info` call will
  be seeing the state as it was when the tasks were submitted, unless it walks
  the sidecars."* — the tension the "halfway house" fallback (above) has to
  resolve.
- **Concurrency-of-invocation question** — asked what happens if a user runs a
  remakefile while jobs are still queued/running on SLURM, and whether array jobs
  have changed the answer.

### Concurrency bugs found in anger

- **Shared-log corruption under array writers.** Identified that
  `.remake/remake.log` (a single loguru file sink) interleaves and garbles under
  concurrent SLURM array writers on NFS — a real bug invisible to single-process
  runs — and directed the move to **per-task log files**
  (`.remake/tasks/log/<rule>/<key[:2]>/<key[2:]>.log`).
- **Upstream-failure propagation, tested for real.** Asked for the
  failure-propagation test to be drafted and for the **`--kill-on-invalid-dep`**
  sbatch-template change, so a failed array element actually stops its dependents
  rather than letting them run into missing inputs.
- **Multiproc CPU detection on shared nodes** — questioned whether the multiproc
  executor needed checking on a high-CPU JASMIN node, then had it switched to
  `os.sched_getaffinity` so it respects the cores actually allocated rather than
  the machine's total.

### CLI and dependency philosophy

- **`remake why` across many tasks** — recalled remake2's `remake info
  --reasons` (the per-task "why") and proposed surfacing it for *all* matching
  tasks, not one.
- **Deduplicated failure reporting** — proposed shortening remake2's unwieldy
  `remake info -F` to *unique failures plus a count*. When offered Drain3 for the
  log-template dedup, steered to a lighter touch: *"Drain3 is an approach, but use
  the original. Let's keep deps down."* — a standing **dependency-minimalism**
  rule for the project.

### Dogfooding the skill on real pipelines

- Drove the first real-world exercise of the remake skill and the hand-migration
  workflow: copying `wescon-tools` and migrating its `ctrl/remakefiles/` from
  remake2 to remake3 by hand (LLM), validating the migration guide against
  genuine Style-A `Rule` pipelines rather than toy examples.

---

*Compiled by Claude Code from the project's session transcripts at Mark's
request — the original sections on the local development machine, the "Field
validation on JASMIN" section on the HPC machine. The framing throughout: Mark
Muetzelfeldt is the designer and decision-maker; Claude Code implemented under
his direction, starting from the remake2 codebase.*
