# Changelog

All notable changes to remake are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Fixes from the 2026-09-24 full-implementation review (IDs refer to
`design_docs/code_reviews/2026-09-24_review.md` on `main`).

### Fixed

- **multiproc and dask runs now record `run_seq`** (H1). Their workers
  stored it as NULL, which silently switched off the durable rerun
  propagation of 0.8.0 (bug 01): after rerunning only an upstream rule
  (`run -Q "rule == 'a'"`), downstream tasks were never rerun under
  `-E multiproc`/`-E dask`. Records already written as NULL stay unmeasured
  (nothing reruns on upgrade); new runs are tracked.
- **A task calling `sys.exit()` is now a recorded task failure** (H6).
  `SystemExit` from task code — typically a CLI `main()` — escaped every
  executor and ended the whole run silently: `sys.exit(0)` gave exit code 0
  with the remaining tasks never run and nothing recorded. A non-zero
  `sys.exit()` is now recorded as a failure (traceback stored) and the run
  continues; `sys.exit(0)` / `sys.exit()` — a wrapped CLI succeeding — is
  success. Ctrl-C (`KeyboardInterrupt`) still stops the run.
- **Failures before the rule function runs are recorded** (M11): creating
  output directories and resolving callable `inputs`/`outputs` happened
  outside the failure handling, so e.g. an output path under an existing
  *file* failed with no traceback anywhere and the task showed as pending.
- **Failure tracebacks reach the per-task logs** (M12): they are logged at
  DEBUG (so `remake task-log` shows them for multiproc/dask/SLURM tasks)
  without adding a traceback per failure to the console.
- **Duplicate rule names are an error** (H8). Task keys and DB records are
  keyed by rule name, so two different rules sharing a name (two modules
  each defining `process`, or a clashing `name=`) silently shared records:
  the plan listed each task twice, the rules overwrote each other's code
  record so they reran on every invocation, and multiproc/SLURM ran the
  first rule for both. Registering such a pipeline now raises `RemakeError`
  naming both definitions. Re-registering the *same* function (a notebook
  cell executed again) replaces the earlier rule with a warning instead of
  doubling the task list.
- **A half-created `.remake/remake.db` no longer bricks the pipeline** (M5).
  Whether to create the schema was decided by the file existing, but SQLite
  creates the file before the schema is written — so a first run killed
  mid-create (disk/quota full, Ctrl-C) left a file on which every later
  command failed with `no such table: task` until it was deleted by hand.
- **Schema creation and migrations are one transaction** (M6). Statements
  used to commit one at a time, so concurrent first opens after an upgrade
  (SLURM continuation jobs, `info` next to `run`) raced — `duplicate column
  name` / `table ... already exists` — and an interrupted migration left a
  half-migrated DB that was skipped forever (for a 0.8.0a0-era DB: every
  task of a rule with `uses=` rerunning on every run). Now concurrent
  openers queue on the lock, an interrupted migration is rolled back and
  redone on the next open, and a DB left partially created by an older
  remake is completed.
- **Lock retries are bounded and only retry lock errors.** Any SQLite
  `OperationalError` (e.g. `no such table`, disk full) used to be retried
  forever with growing backoff — a silent hang; they are now raised at
  once, and a DB still locked after 10 minutes is a clear error.
- **`run` exits non-zero when rules stay blocked** (M9). A deferred
  (`@deferrable`) matrix that never resolves — a mistyped path, or an
  upstream that failed — only logged a warning and exited 0, so scripts and
  CI read an incomplete run as success. It now exits 1 and each blocked rule
  is reported with the reason: the paths its `Defer` is waiting on, or that
  an upstream rule did not complete. `Remake.blocked_rules` lists them after
  `run()` (whose return value is unchanged: the number of failed tasks).
  In a filtered (`-Q`) run, a rule blocked only because the query left out
  an upstream that still has work is reported as a warning and does not
  change the exit code.
- **Queries (`-Q`) no longer fail silently** (M14). A name that is no
  rule's matrix key — almost always a typo, e.g. `-Q "yera == 2000"` — used
  to match nothing, so `run` said "Nothing to do" (exit 0) and `set-state`
  quietly changed nothing; it is now an error listing the valid keys.
  Syntax errors and evaluation errors (e.g. comparing an int to a str) are
  clean `error:` messages (exit 2) instead of tracebacks. A few safe
  builtins are available — `range`, `len`, `min`, `max`, `abs`, `int`,
  `str`, `set`, … — so `-Q "year in range(2000, 2005)"` works (it silently
  matched nothing before). A query that matches no tasks at all is warned
  about. (The restricted-parser replacement for `eval` remains a todo.)
- **An older sidecar result can no longer overwrite a newer record** (M17).
  A result file left un-ingested (a SLURM element, or a multiproc run whose
  parent died) was applied unconditionally on the next ingest, reverting a
  later direct write — e.g. a successful `remake run-task` flipped back to
  failed. Ingest now only applies results from the same or a later
  invocation (by `run_seq`, not wall clock, so node clock skew can't
  reorder them).
- **Malformed sidecar results are quarantined** (L14). A result file of
  valid JSON but the wrong shape crashed every `plan`/`info`/`run` until
  deleted by hand, and an unreadable one was warned about on every command.
  Both are now renamed to `*.json.bad` with a single warning.
- **Console logs honour `--colour never`, `NO_COLOR` and non-TTY stderr**
  (L25). They were always ANSI-coloured (and double-bolded), filling CI logs
  and SLURM `.err` files with escape codes.
- **Piping into `head` no longer ends in a `BrokenPipeError` traceback**
  (L26); remake stops quietly with exit code 141.
- **User errors are clean `error:` messages with exit code 2** (L27) instead
  of tracebacks with exit 1 — the code that means "tasks failed". Covers a
  missing remakefile, a directory or non-`.py` path given as the remakefile,
  an unknown `depends_on` name or a dependency cycle (now `RuleGraphError`,
  still a `ValueError` for existing handlers), an unloadable `-E
  module:Class`, a corrupt `.remake/remake.db`, an unwritable `.remake/`,
  and an out-of-range `run-array-task` index. Exit codes are documented in
  the running guide (0 success, 1 tasks failed or rules blocked, 2 usage
  error).
- **`depends_on='name'` (a bare string) works** (L4); it was split into
  single characters (`unknown rule 'e'`). A single `Rule` is accepted too.
- **Tasks are skipped when an input they read failed to be rebuilt** (H3,
  failure-skip part). With a shared matrix, only the same-kwargs downstream
  task was skipped after an upstream failure; a task reading a *different*
  upstream element (e.g. `inputs` of `year - 1`) still ran — on that
  element's stale output if one existed from an earlier run. Local
  executors now also skip any task whose declared inputs include an output
  of a task that failed this run. (Rerun propagation for such pipelines is
  the 0.9 fix.)
- **Concurrent local runs in one directory are prevented** (M15). Two
  `remake run`s started together both executed every task and raced on the
  same outputs. `run` now holds `.remake/run.lock`; a second run gets a clear
  error (exit 2). A lock left by a crashed run on the same host is detected
  and replaced; one from another host must be deleted by hand if stale (the
  error says so). SLURM submissions (`-E slurm`, incl. continuation jobs)
  take no lock; they keep their own duplicate-submission guard.

## [0.8.3] — 2026-07-14

### Fixed

- **The `remake[examples]` extra shipped broken in 0.8.2**: h5netcdf ≥ 1.8
  made its h5py backend optional, so a clean
  `pip install "remake[examples]"` couldn't open NetCDF files
  (`ImportError: No module named 'h5py'` from `make_example_data.py`).
  The extra now depends on `h5py` explicitly.
- The bundled examples are now held warning-clean by the test suite
  (warnings are test failures), which caught and fixed five unclosed file
  handles in ex3/ex4.

### Added

- `Sqlite3Backend` gained `close()` and context-manager support
  (`with Sqlite3Backend(...) as meta:`), and closes its connection on
  garbage collection instead of emitting a `ResourceWarning`.

## [0.8.2] — 2026-07-10

### Added

- **`remake[examples]` extra**: `pip install "remake[examples]"` (or
  `uv add "remake[examples]"`) installs everything the bundled examples need
  beyond remake's minimal core — `xarray`, `netCDF4`, `h5netcdf`, `zarr<3`,
  `dask`, `pyyaml`.

### Fixed

- **The bundled examples no longer clash with each other.** Six rule names
  were duplicated across the example files, so working through the examples
  in one directory tripped the duplicate-rule-name warning and clobbered
  recorded state (rerunning an earlier example reran everything). Every rule
  name is now unique across the set. `ex5_multifile` also ran from its own
  directory against input data that `make_example_data.py` only wrote to the
  parent directory — the generator now populates both.
- The examples README described pre-0.8.0 path behaviour; rewritten for the
  current rule (`.remake/` and data anchor to the remakefile's directory).
  Stale example filenames in the docs fixed.

## [0.8.1] — 2026-07-10

A SLURM-executor hardening release: the fixes from an adversarial review
of the submission logic. No schema changes, no new rerun triggers —
upgrading does not cause an existing pipeline to rerun anything.

### Fixed

- **A failed `squeue` is no longer mistaken for an empty queue.**
  `squeue` failing, missing, or hanging (60s timeout) used to look like
  "nothing queued", green-lighting duplicate submission of arrays still in
  flight. It now raises a distinct error: `remake run -E slurm` refuses to
  submit when previous submissions are recorded (proceeds with a warning on
  a fresh directory), and `remake slurm-status` reports the failure instead
  of showing every job as "not in queue".
- **Job specs are per-submission and immutable**
  (`.remake/jobs/<rule>.<run_seq>.json`; each sbatch script pins its own
  submission's file via `run-array-task --specs`). Replans and dry runs
  write a fresh file, so a queued array always executes the exact task list
  it was submitted with — the spec-rewrite index corruption is structurally
  impossible. `task-info`'s array-index mapping survives replans. Spec files
  older than 7 days are pruned automatically (each rule's last-submitted
  spec is kept at any age).
- **`remake resubmit` is guarded**: it now refuses when jobs from the last
  submission are still queued (duplicates racing on the same outputs), when
  `squeue` fails, or when `submit.sh` bakes in literal dependency job ids
  that have left the queue (sbatch would reject them mid-script) — each with
  a pointer to `remake run -E slurm`, which replans correctly.
- **`aftercorr` (element-wise) dependencies are now proved, not assumed.**
  Previously chosen when upstream/downstream kwargs matched, which let a
  stencil-style rule (task *n* reads upstream *n−1*, *n*) start elements
  against unwritten neighbour outputs, and let elements racing on a shared
  upstream output (e.g. one zarr store) start early — silent partial data.
  Correspondence is now derived from resolved task inputs/outputs (pairwise
  disjoint upstream outputs, every element reading its counterpart), falling
  back to `afterok` (wait for the whole upstream job) when unprovable.
- **Suspended/held jobs count as queued.** The active filter accepted only
  PD/R/CF, so a suspended or requeue-held array looked finished and was
  double-submitted; it now treats every non-terminal state as active.
- **Continuation jobs can no longer self-replicate or hang forever**: a
  continuation is only submitted when there is something to wait on, and it
  carries `--kill-on-invalid-dep=yes` so a failed upstream cancels it
  instead of leaving it pending as `DependencyNeverSatisfied`.
- **`run-array-task` verifies the rebuilt task key** against the submitted
  spec's `task_key`, failing loudly instead of recording a result under a
  key the planner never reads (task stuck pending forever).
- **Robustness**: a truncated jobids sidecar (killed script, full disk) is
  treated as absent with a warning instead of crashing every SLURM command;
  remakefile paths containing spaces are quoted in generated scripts.

### Changed

- **`remake info` columns are now a four-state partition**: `up-to-date`,
  `stale`, `failed`, `pending` (replacing `success`, which silently included
  successes the planner would rerun). The counts satisfy
  `up-to-date + stale + failed + pending = tasks` and
  `up-to-date + to run = tasks`. In `--json`, the per-rule/totals key
  `success` is replaced by `up_to_date` and `stale` is added. (A CLI-output
  change in a patch release — recorded exception in
  `design_docs/compatibility.md`: the old count was misleading and the break
  is loud.)
- **Every SLURM rule is submitted as one array job**, even a single task
  (`--array=0-0`); the separate individual-job mode is gone. Sidecars always
  record `slurm_array_job_id` (old `slurm_job_ids` sidecars are still read,
  so in-flight submissions survive the upgrade). The `array_threshold`
  config key is obsolete and ignored.

## [0.8.0] — 2026-07-03

**remake 0.8.0 is a ground-up rewrite.** It shares the name and the core idea
of the remake2 `0.6.x` line — declarative, code-aware, make-style incremental
rebuilds in pure Python — but the API, the engine and the on-disk metadata are
all new. Treat it as a new tool rather than an in-place upgrade; see
*Migrating from remake2* below.

(Published to PyPI as the `0.8.0a0` alpha during development; `0.8.0` is the
first stable release of the rewrite.)

### Added

- **Lazy, rule-level engine.** A pipeline is a set of `@rule`s, each expanding
  to many tasks via a `matrix`. Tasks are never materialised in bulk — the
  planner works rule-by-rule — so pipelines of ~1e6 tasks plan in seconds with
  flat memory (benchmarked in `tests/benchmarks/`).
- **The `@rule` decorator API**: `inputs=`, `outputs=`, `matrix=`,
  `depends_on=`, `uses=`, `config=`, `strict_scope=`, and `name=`. Rules are
  free-standing objects registered via `rules_from_current_module()`,
  `rules_from_modules()`, `add_rules()`, or `Remake(rules=[...])`.
- **Matrix forms**: dict cartesian product, explicit `list[dict]`, tuple-keys
  for pre-filtered / grouped combinations, plain callables, and `@deferrable`
  callables for **dynamic matrices** whose task set is derived from upstream
  outputs (raising `Defer` until the upstream is ready).
- **`depends_on=` by name**: dependencies may be given as rule objects or as
  string rule names, resolved at finalize time.
- **Smart stale-rebuild via content, not timestamps.** A task reruns when its
  run-code, its `uses=` values, or its `inputs`/`outputs` spec change — each
  compared by AST-normalised hashing (`run` code, `uses_hash`, `io_hash`), so
  cosmetic edits don't trigger reruns and mtime is never consulted.
- **`uses=` scope tracking** with decoration-time scope analysis: undeclared
  outer-scope names used by a rule are flagged (`ScopeWarning`, or
  `ScopeError` under `strict_scope`).
- **Output tokens**: `FileToken`/`PathToken`, `ZarrStore`, `S3Object`, and a
  user-extensible `OutputToken` protocol (`identity()` / `is_complete()` /
  `format()`) for non-file outputs.
- **Executors**: `singleproc`, `multiproc`, `slurm`, and a basic `dask`
  executor — plus injection of a custom `Executor` via dotted path
  (`mymodule:MyExecutor`).
- **SLURM support built for HPC reality**: array jobs, self-replanning
  continuation jobs (submit-and-log-out), per-rule resource `config`,
  `aftercorr`/`afterok` failure propagation, and **JSON sidecar result files**
  ingested in a single batched transaction — validated lock-free at 800-way
  concurrency on JASMIN, where direct SQLite writes livelock. Job ids are
  persisted as sidecars and consumed by `slurm-status`, `task-info` and
  resubmission.
- **Per-task log files** under SLURM arrays (`.remake/tasks/log/...`), avoiding
  the interleaved-write corruption of a single shared log.
- **CLI**: `run`, `info`, `why`, `lint`, `set-state`, `ls-tasks`, `rule-info`,
  `task-info`, `task-log`, `rule-dag`, `slurm-status`, `resubmit`, `version`,
  plus the internal `run-task`/`run-array-task` entry points.
  - `why` explains why any task (or a `-Q` query set, or the runnable set)
    will or won't rerun — showing the *substance* of each change: a unified
    diff of the rule body, per-helper source diffs and `old → new` values for
    `uses=` changes (raw helper sources are kept in a per-rule manifest so old
    versions can be diffed), and which `inputs`/`outputs` segment differs.
  - `rule-info` shows one rule's docstring, matrix, input/output templates
    and `uses` (the rule's docstring is surfaced on the `Rule` object too).
  - `info --reasons` gives a per-rule tally of rerun-reason categories;
    `info -F` groups failed tasks by traceback signature; `info` prints a
    totals row in dependency order.
  - `run -Q` to scope a run; `run --dry-run`; `run --force`;
    `run --ignore-code-changes` (rerun only never-succeeded tasks);
    `run -X/--debug-exception` to drop into pdb on the first failure;
    `run --raise` to re-raise the first failure with its traceback (no
    debugger — for CI and scripts).
  - `set-state --success`/`--pending` (with `-Q`) to record state without
    running; `--success --check-outputs` adopts an existing output tree;
    `--success` cascades to downstream complete tasks by default (guarded),
    with `--no-cascade` to opt out.
- **Programmatic API mirroring the CLI.** Every read-only command and
  `set-state` is also a method on the `Remake` object, returning structured
  data rather than printing: `status_summary()` (info), `lint()`,
  `task_info()`, `rule_dag()`, `why()` / `explain_tasks()`, `select_task()`,
  and `set_state()`. The CLI is a thin rendering layer over these, so a
  pipeline can be driven and inspected entirely from Python (e.g. in a
  notebook) without shelling out.
- **Duplicate-rule-name guard.** The `rule` table records which remakefile
  defined each rule; when two co-located remakefiles (sharing one `.remake/`)
  define a *different* rule under the same name — which would otherwise silently
  clobber each other's state, logs and SLURM job specs — `ensure_rules` warns,
  distinguishing a collision from an ordinary same-file edit.
- **`-T`/`-D`/`-I`/`-W` logging levels** (trace/debug/info/warning), a
  `--colour {auto,always,never}` flag (`NO_COLOR`/`FORCE_COLOR` honoured;
  info/ls-tasks/task-info/why output is colourised on TTYs), and **three
  rotating file logs** next to the metadata DB: `.remake/remake.log` (the
  human-facing run narrative, INFO+), `.remake/remake.debug.log` (the DEBUG
  firehose), and `.remake/remake.jsonl` (a structured mirror — real fields
  per event plus a per-invocation `run_id`, minable with `jq`). Runs end with
  a summary line (`ran N task(s), M failed in X.Xs`) and per-task durations
  are logged on completion/failure. Set `REMAKE_LOG_CODE=1` to dump full
  code-comparison bodies at TRACE (off by default — they crowd out the log).
- **SQLite metadata backend** (`.remake/remake.db`) with a defensive
  `ALTER TABLE` migration path; pluggable via the `MetadataManager` interface.
  Task rows reference shared rows in a `code` table (FKs, not inline text), so
  the DB stays compact and status queries stay flat as task counts grow; an
  invocation-scoped record cache means `info`/`why` fetch each task record at
  most once.
- **Documentation site** (MkDocs), **CI** across Python 3.10–3.14 with
  coverage, and a Trusted-Publishing release workflow.

### Changed

- **`check_outputs` defaults to `'never'`** (DB is the sole source of truth).
  An on-disk output is no longer implicitly adopted when there is no DB
  record, so editing a task's code and clearing its record with
  `set-state --pending` reliably forces a rerun instead of silently re-adopting
  the stale output. The previous `'fallback'` behaviour is now the opt-in
  *migration* mode; adopt an existing tree explicitly with
  `run --check-outputs` or `set-state --success --check-outputs`. The three
  modes (`'never'`, `'fallback'`, `'always'`) are documented in the running
  guide.
- **`run --ignore-code-changes` is long-form only** — its former `-I` short
  flag collided with the global `--info`/`-I`, so `-I` now unambiguously means
  info-logging.
- **Rule plumbing mistakes fail once, at definition.** A rule whose signature
  doesn't match its declared specs and matrix keys, whose `inputs`/`outputs`
  template references a `{placeholder}` the matrix doesn't provide, or whose
  callable spec requires a parameter the matrix lacks now raises
  `SignatureError` at decoration (at first expansion for callable matrices) —
  naming the rule and the offending field — instead of every task failing
  identically at run time.
- **A `uses=` key that shadows a module global bound to a *different* value
  warns at decoration** (`ScopeWarning`). Inside the rule the `uses=` value
  wins, so the reader sees one definition while another runs; the standard
  tracking idiom `uses={'helper': helper}` (same object) stays silent.
- **One-time in-place DB migration.** The first remake command that touches a
  `.remake/remake.db` created by an earlier `0.8.0` alpha migrates it to the
  code-table layout and VACUUMs — a one-off pause (longer for big DBs) after
  which the DB is substantially smaller. No action needed; don't interrupt it.
- **Commands run from the remakefile's directory.** `remake <cmd> sub/pipeline.py`
  now changes into `sub/` first, so `.remake/`, the log, and the pipeline's
  relative input/output paths anchor to the remakefile rather than the
  invocation cwd (no stray `.remake/` left in the launch directory). cwd is
  restored after the command; running from the remakefile's own directory is
  unchanged.

### Fixed

- **Durable upstream→downstream rerun propagation.** Previously the
  "an upstream reran, so rerun its consumer" signal lived only within a single
  plan, so running an upstream without its consumer in the same pass — via a
  targeted `run -Q`, or a crash between the two — left the consumer falsely
  up-to-date, silently serving output built from stale input. A monotonic
  `run_seq`, stamped on every task per invocation (and threaded through SLURM
  sidecars), now makes the signal durable: a task reruns when an upstream's
  stamp is newer. `remake why` reports this as **upstream-newer**, and
  `set-state --success` is the supported way to declare a consumer still valid
  (it stamps it current, cascading to downstream as above). See
  `design_docs/bugs/01_durable_rerun_propagation.md`.
- **User-facing errors print cleanly.** A `RemakeError` (bad query, a
  single-task command matching >1 task, an unknown rule/executor, a missing
  `submit.sh`, …) now prints `error: <message>` to stderr and exits 2, instead
  of dumping a traceback. The full traceback is still shown under
  `-X/--debug-exception`.
- **SLURM JSON round-trip task-identity bug.** Non-JSON-stable matrix kwarg
  values (e.g. tuples) changed a task's key/paths after the JSON round-trip on
  the compute node, so sidecars were recorded under a key the planner never
  looked up. Such values are now rejected at plan time with a clear
  `SignatureError`, before any submission.
- **`inputs=`/`outputs=` changes now invalidate a task** (`io_hash`) — editing
  an output path no longer leaves orphaned outputs while downstream looks
  elsewhere.
- **SLURM sidecar results now record the code that actually ran.** Editing a
  rule between a SLURM run and the next remake invocation previously stamped
  the completed task with the *edited* source at sidecar ingest, so the change
  was reported as "code unchanged" and the stale output never regenerated.
  The sidecar now carries the run source from the compute node, symmetric
  with `uses`/`io` (see `design_docs/bugs/05_...`).
- **Code comparison degrades safely.** Any parse error in the code comparer
  now counts as "changed" (forcing a rerun) rather than raising — a
  comparison failure can cost a redundant rerun, never a stale skip.
- **SLURM exit-code masking.** The per-rule wrapper no longer masks a task's
  real exit code with a trailing `echo`, so `aftercorr`/`afterok` correctly
  block dependants of failed elements (`--kill-on-invalid-dep=yes`).
- Downstream tasks of a same-run failure are **skipped (and reported)** rather
  than run and failed on missing inputs; skipped tasks stay pending so fixing
  the upstream picks them up.

### Migrating from remake2

remake3 is not a drop-in upgrade — remakefiles must be rewritten to the new
`@rule` API, and the on-disk metadata format is new (start from a fresh
`.remake/`, adopting existing outputs with `set-state --success
--check-outputs`). A step-by-step remake2→remake3 translation guide ships with
the `remake` Claude skill (`references/remake2_to_remake3.md`). The rewrite was
validated by reproducing a real multi-figure paper pipeline (`mcs_prime`)
end-to-end on JASMIN with outputs identical to the remake2 reference.

[0.8.3]: https://github.com/markmuetz/remake/releases/tag/v0.8.3
[0.8.2]: https://github.com/markmuetz/remake/releases/tag/v0.8.2
[0.8.1]: https://github.com/markmuetz/remake/releases/tag/v0.8.1
[0.8.0]: https://github.com/markmuetz/remake/releases/tag/v0.8.0
