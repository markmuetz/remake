# Changelog

All notable changes to remake are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **`-Q` queries use a restricted grammar.** The query is parsed and every
  node checked against an allowlist before it is evaluated (with no
  builtins). Allowed: names (matrix kwargs and `rule`), constants, lists and
  tuples, comparisons (`== != < <= > >= in, not in, is, is not`), `and` /
  `or` / `not`, and unary minus. Rejected with a `RemakeError` (exit 2):
  function calls, so the builtins `len`, `range`, `str`, `any`, ... are gone;
  arithmetic; attributes, subscripts, sets, dicts, comprehensions, lambdas
  and f-strings. `-Q True` still selects every task.

### Changed

- **Dependency matching between tasks uses canonical paths** (0.11 item 3).
  An input that spells its producer's output differently (absolute vs
  relative, `./`, a symlinked directory) now links to the one upstream task
  that writes it, instead of to every task of the upstream rule. Pipelines
  that relied on that fallback rerun fewer tasks; never fewer than needed.
- Canonicalising paths looks up the working directory once per check
  instead of once per path (`getcwd` costs ~60 µs on macOS): lint and the
  missing-inputs check over 1e6 paths drop from ~100 s to ~2 s there.

- **SLURM job specs are format 2** (`.remake/jobs/<rule>.<run_seq>.json`):
  one header (`v`, `rule`, `run_seq`, `env`) and a `tasks` list of
  `{task_key, kwargs, reason}`, instead of a list repeating every field per
  task. Format 1 files are still read, so arrays queued by 0.9/0.10 run
  after an upgrade; a 0.10 remake cannot read a format 2 file (don't
  downgrade with jobs queued). Tools reading the files directly: use
  `remake.executors.slurm_executor.load_job_specs`.
- **`remake history --prune` also deletes stale per-task logs**: those of
  tasks no longer in the pipeline (a changed matrix, a removed rule). Its
  `--json` output gains `logs_deleted`. New `Remake.prune_task_logs()`.
- compatibility.md lists which files under `.remake/` are the on-disk
  contract and which are scratch.

### Added

- **`remake info` counts rules deferred only by an upstream rule** (soak
  finding 2, 2026-11-23). A rule with a static matrix that waits on a
  deferred rule upstream now shows its task count, and failed and pending
  from the DB, with `to run` naming the rules holding it (`deferred
  (process_event)`); a line under TOTAL counts the deferred tasks. Its
  `--json` row gains `tasks`, `succeeded`, `failed` and `pending`; every
  deferred row gains `deferred_by` (`[]` when its own matrix is the cause),
  and `totals` gains `deferred_tasks`. A rule whose matrix is callable
  still shows `?`.
- **`remake lint` checks inputs against deferred rules' output templates**
  (soak finding 3). An input that a deferred rule would produce was reported
  as a near miss of an unrelated file, or as external, hiding a missing
  `depends_on`. It now matches the deferred rule's output templates, so the
  missing dependency is reported. A callable outputs spec is called with a
  placeholder for each parameter it names to get its templates. A deferred
  rule whose outputs still can't be read is named once under the findings,
  and on `near_miss` and `external` rows in `--json` (`unchecked`).
  A rule that waits on a deferred rule is now checked too (it was skipped),
  and a rule whose inputs can't be resolved yet is skipped with a warning
  instead of crashing lint.
- `remake lint` no longer creates `.remake/remake.db` in a fresh checkout: it
  reads no metadata, so it doesn't open the database.
- **`remake lint` shows where a near miss differs** (soak finding 4): the two
  paths stacked and aligned, the shared prefix collapsed to `…`, the
  difference coloured, or marked with `^` without colour. Compared by
  directory, so an extra directory doesn't misalign the rest.

### Fixed

- **`uses` values that refer back to themselves no longer hang the hash**
  (soak finding, 2026-11-23). An object holding several bound methods of
  itself, such as `scipy.stats.chi2`, was rendered in time exponential in
  the depth limit, so loading the remakefile never finished (`remake info`
  ran >12 min at 10 GB). An object met again inside its own rendering now
  renders as `<cycle T>`. Rules whose `uses` hold such a self-referencing
  object get a new hash and rerun once after upgrading from 0.9/0.10;
  other `uses` values hash as before. Such an object (a library instance
  whose repr has an address) is still hashed by its whole `__dict__`, so
  upgrading the library that defines it (scipy, here) may rerun the rule.

### Deprecated

- `Dir.claim()` now warns (`DeprecationWarning`); use `Dir.adopt()`. Removed
  at 1.0, with `Remake.status_summary()`, `Remake.task_spec()` and the
  built-in JASMIN SLURM defaults used when no profile is selected (see
  design_docs/releases/v0.11.0.md item 7).

## [0.10.0] — 2026-11-22

Provenance and run history, `remake verify`, list-valued inputs and
outputs, `metadata_dir`, and the surfaces that 1.0 will freeze: the
output-token interface, `--json` output, exit codes and `Remake` method
names. A `Remake` used from Python now anchors at its defining file, as the
CLI does. Upgrading from 0.9 reruns nothing (checked on the examples and on
a 13,000-task field pipeline on JASMIN); the database schema moves to 1.1
in place and 0.9 can still read it. Python 3.11 is now the minimum.

### Breaking

- **A `Remake` used as a library anchors at its defining file** (0.10
  decision D6), as the CLI does: relative paths, `.remake/` and
  `remake.toml` resolve against the directory of the script that builds
  the `Remake`, not the cwd. `Remake(project_dir=...)` overrides; a
  notebook or REPL anchors at the cwd. A script run from another directory
  used to write beside the cwd; it now writes beside the script.
- **Module scans claim only rules defined in the scanned file.**
  `rules_from_current_module()` and `rules_from_modules()` no longer claim a
  Rule imported from another module (it could land in the wrong pipeline).
  Remakefiles that composed a pipeline by importing rules by name must
  register them with `rmk.add_rules(...)` or `rmk.rules_from_modules(...)`;
  `examples/ex5_multifile` now does the latter. Rules imported, or held in
  lists, tuples or dicts, are not claimed and are named in one warning.
- **Every `--json` output is now an object with `"format": 1` as its first
  key** (documented in `docs/cli.md`, "JSON output"). Bare lists became
  objects: `info --failed --json` is `{"format": 1, "failures": [...]}`,
  `ls-tasks --json` is `{"format": 1, "tasks": [...]}`, `lint --json` is
  `{"format": 1, "findings": [...]}`, and `slurm-status --json` is
  `{"format": 1, "submissions": [...]}`. `info`, `rule-dag`, `rule-info`,
  `task-info`, `config` and `check-env` already printed objects and only gain
  `"format": 1`.
- `OutputToken.after_success` marks a token that summarises many files
  with `'aggregate': True` (was `'dir': True`); 0.9 sidecars are still
  read. Custom tokens that are neither local paths nor `remote = True` are
  now asked `is_complete()` by the missing-inputs check (0.9 skipped every
  non-path token); set `remote = True` on a token class to opt out.

- **Exit code 3 for a run that stopped short** (0.10 decision D5): when no
  task failed but rules were left blocked (a deferred matrix never became
  ready), `remake run` now exits 3, not 1. A failed task is still 1, also
  when rules were blocked too. `run-all` follows the same rule and lists
  such remakefiles as `blocked`. Scripts that test for `1` to mean "not
  finished" should also accept 3.
- **Python 3.10 is no longer supported; 3.11 is the minimum.** Python 3.10
  reached end of life in October 2026. The `tomli` fallback is gone
  (`tomllib` is built in), and the lock now resolves tornado 6.6 or later.

### Added

- **`metadata_dir` puts `.remake/` elsewhere** (0.10 item 5f, decision D7): a
  project-file key (`remake.toml`, or `Remake(config=)`) for the metadata
  directory, e.g. on a scratch filesystem with more quota. Relative values are
  relative to the project directory; `~` and environment variables are
  expanded. The database, logs, results, run history and SLURM scripts, job
  specs and `submit.sh` all move with it, and workers and array jobs find it
  by loading the remakefile. Default unchanged (`.remake/` beside the
  remakefile). Internally every `.remake` path now comes from
  `remake.util.paths`. The CLI's own log files follow only the `remake.toml`
  key. See the configuration guide.
- **List-valued inputs and outputs** (0.10 decision D8; bug 02, review
  L2): `outputs={'parts': ['a/{i}.nc', 'b/{i}.nc']}` names several paths
  under one key, formatted per element; the rule body gets a list. Lint,
  `task-info` (as `parts[0]`, `parts[1]`), `ls-tasks`, `verify`, the
  missing-inputs check and dependency tracking see each path. Lists must be
  flat. A dict spec that already held a list used to be stringified into
  one bogus path (such a pipeline could not run); it now means the list.
  `task-info` no longer crashes on a tuple from a
  callable spec. Example: `examples/ex17_list_values.py`.
- **`remake verify`** (0.10 item 2): reconciles the DB with the disk.
  `--outputs` finds tasks recorded as success whose outputs are missing or
  fail `Ensure` (marked pending with `--apply`); `--adopt` finds unrecorded
  tasks whose outputs are complete (recorded as success with `--apply`).
  Read-only by default, `-Q`-composable, `--json`, batched completion
  checks. `Remake.verify()` is the library form. `set-state
  --check-outputs` now points at it.
- **The output-token interface is settled for 1.0** (0.10 item 5a,
  `remake.core.tokens`): `OutputToken.stat()` (an `OutputStat` of
  exists/size/mtime, without reading the data), `complete_many()` (batched
  completion: local paths take one directory listing per directory instead
  of one stat per file, used by `check_outputs` planning), `canonical()`
  (one spelling per path, used by lint, the missing-inputs check and `Dir`
  ownership, so `./a`, `a` and `/abs/a` are one file there; a symlinked
  `Dir` also matches through its target. Dependency matching between tasks
  still compares normalised paths, and falls back to depending on every
  upstream task when spellings differ), `adopt()`
  (`Dir.claim` remains as an alias), and a `remote` class flag. `S3Object`
  is marked experimental: storage backends come after 1.0.
- **Provenance and run history** (0.10 items 1 and 3): each run records the
  git commit, a dirty flag and diff hash, Python and remake versions, an
  installed-distribution fingerprint, a lockfile hash, the host, and a hash
  of the username. Each task execution is kept (the last `history.keep`,
  default 10, per task) with its rerun reason, resources, host and error
  signature. New `remake history <task>`, `remake history --runs` and
  `--prune`; `task-info` shows the last execution and flags a SLURM job that
  ran in a different environment from the planner. Schema 1.1, additive:
  upgrading reruns nothing.
- `remake.__all__` declares the public API, checked against `docs/api/` by a test.

### Changed

- **`Remake` method names match the CLI** (0.10 item 5d):
  `status_summary()` is now `info()` and `task_spec()` is `export_task()`;
  the old names still work, with a `DeprecationWarning`, until 1.0. New
  `Remake.history()` is the library form of `remake history`. The
  missing-inputs check and `verify` check files in batches (one directory
  listing per directory, 0.10 item 4).
- **One environment report.** The SLURM submit header and `check-env` now
  share one function (`envcheck.environment_report`), which also carries the
  environment fingerprint, the installed packages and the lockfile hash. The
  submit header gains `remake`, `environment fingerprint` and `lockfile`
  lines. `check-env` compares the fingerprint and says `OK  same packages`
  or lists the `name==version` entries that differ.

## [0.9.0] — 2026-11-21

Canonical change detection, a config file cascade with profiles, output
checks (`Ensure`), directory outputs (`Dir`), and the October 2026 field
notes. Upgrading reruns nothing: records from 0.8 are matched and restamped.
The database schema is now versioned (see Changed). Keep 0.8 and 0.9 off
the same `.remake/` directory.

### Added

- **Output checks with `Ensure`** (0.9.0 item 4):
  `outputs={'nc': Ensure('out/{year}.nc', min_size=1024, checksum=True)}`
  checks the output when its task finishes (`nonempty`, `min_size`); a
  failed check fails the task. `Ensure` returns the same kind of token, its
  options are not part of the output's identity (adding one reruns
  nothing), and `check_outputs`/`set-state --check-outputs` apply them before
  adopting a file. **Opt-in checksums** (`checksum=True`, or
  `checksum_outputs = true` in rule/pipeline config) record sha256 at
  completion, also through SLURM sidecars; they are recorded, never
  compared. `remake task-info` shows output stats.
- **`Dir` outputs** (0.9.0 item 5, `design_docs/designs/dir_outputs.md`):
  `outputs={'plots': Dir('plots/{year}')}` declares a directory a task fills
  with a variable number of files. Completion is a hidden
  `.remake-manifest.json` written after success; a rerun deletes only the
  manifest, never your files; files the run didn't produce are recorded as
  stale, with a warning. Works with `Ensure(Dir(...), nonempty=, min_size=,
  checksum=)` (per-file checksums in the manifest). Two tasks declaring the
  same `Dir` is an error; `remake lint` matches inputs inside a `Dir` and
  reports nested Dirs and file outputs inside one; `set-state --success
  --check-outputs` adopts an existing `Dir`. `task-info` shows output stats
  and stale counts; `info --json` has per-rule `output_bytes` and
  `info --sizes` a size column. Example: `examples/ex14_dir_outputs.py`.
- **Configuration profiles and config files** (0.9.0 item 7,
  `docs/guide/configuration.md`): a TOML cascade — built-in defaults →
  profile → user file (`$XDG_CONFIG_HOME/remake/config.toml`) → project file
  (`remake.toml` beside the remakefile) → `Remake(config=)` → per-rule
  `config=`. Dicts merge, lists and scalars replace; unknown keys in files
  are errors. A shipped `jasmin` profile (LOTUS2 defaults, no `account`;
  SLURM refuses to submit under it until `slurm.account` is set) selected
  with `--profile`, `$REMAKE_PROFILE` or `profile =`. `remake config
  [--json]` shows the effective config and which layer set each key. The
  effective config is recorded per invocation, and the executor, nproc and
  profile are logged at run start.
- **Fail fast on missing external inputs** (0.9.0 item 6): before running,
  missing inputs that no rule produces stop the run with one error, grouped
  by rule and truncated with a count (exit 2), instead of N
  FileNotFoundError failures. `Remake(config={'external_inputs_hint': ...})`
  appends a pipeline-specific nudge; `-n` warns and still shows the plan.
- **`remake dag [--rules|--tasks] [-o FILE]`** (item 8): the rule graph, or
  the task graph from path-derived dependencies, as Graphviz DOT.
- **`remake run-all [DIR] [-r]`** (item 9): runs every remakefile in a
  directory (found by AST scan, never imported) in sorted order, continues
  past failures, prints a summary and exits non-zero if any failed.
- From the October 2026 field notes (`docs/notes/scaffold-omnium-port.md`):
  - `remake info --failed`: one line per failed task — rule, short key, last
    error line, log path — falling back to the SLURM `.err` when the DB has
    no exception; `--json`.
  - `remake export-task <key>|-Q query [-o FILE]`: a task's resolved spec as
    JSON, to run its body from an interpreter that can't import remake.
  - `remake check-env REMAKEFILE [--srun] [--json]`: compares the
    environment you plan with against a fresh job-style process (optionally
    on a compute node): Python, remake, and every module the remakefile
    imports. Exits 1 on any mismatch.
  - SLURM submissions record the interpreter, `PYTHONPATH`, `sys.path` and
    the files of your own top-level modules in the `.remake/submit.sh` header
    and the run log.
  - `examples/ex13_rule_factory.py`, and guide sections on rule factories,
    format specs in placeholders, input order, shared side outputs, pixi and
    `PYTHONPATH`, resubmitting while a submission is in flight, and `sacct -S`
    on clusters that reuse job ids.


- **Per-task resource capture** (0.9.0 item 1,
  `design_docs/designs/resource_capture.md`): every task execution now records wall
  time, CPU time (user+sys, including waited-for children) and peak resident
  memory, for **all** executors, into `remake.db`. `remake task-info` shows
  them (`resources: wall 12.40s, cpu 11.98s, peak rss 1.4G`) and exposes them
  under a `resources` key in `--json`; the `task_complete`/`task_failed` JSONL
  events carry the same fields. Failed tasks are measured too. Peak memory is
  sampled from `/proc` (100 ms by default) rather than read from `getrusage`,
  which is a *process* high-water mark and would attribute a pooled worker's
  earlier peak to whichever task finished last; where neither is reliable
  remake records nothing rather than a wrong number, and `rss_method` says
  which measurement produced the figure. Disable the sampler with
  `Remake(config={'resources': {'capture': False}})`; wall/CPU time are free
  and always recorded. A task killed by the OOM killer or by SLURM records
  nothing — that remains `sacct`'s job.

### Changed

- **Change detection is canonical and version-independent** (0.9.0 item 2;
  review 2026-09-24 H2, H4, H5, M3, M4, L8). Fewer spurious reruns and
  fewer missed changes:
  - Editing the `@rule(...)` decorator (`matrix=`, `config=`, `name=`) no
    longer reruns the rule's tasks; decorators below `@rule` still count, and
    so do docstrings.
  - `uses` values render deterministically: sets and dict keys sorted (a set
    of names no longer reruns on every invocation), numpy arrays by
    dtype/shape/bytes (no more truncated or rounded reprs), pandas by values,
    index, columns and dtypes, `functools.partial`/ufuncs/builtins/sourceless
    functions by what they compute, containers element by element, objects
    with an address in their repr by their attributes, and a
    `__remake_hash__(self) -> str` hook for your own types. Your own modules
    in `uses` are tracked by source.
  - `inputs`/`outputs` specs are compared by a version-independent AST, so
    moving between Python 3.12 and 3.13+ no longer reruns everything; a
    lambda spec is cut from the line it sits on; reordering the keys of an
    evaluated dict spec is not a change.
  - **Upgrading reruns nothing:** records written by earlier versions are
    matched against the new renderings and re-stamped on the next run.
- **Fan-ins rerun when their callable spec resolves differently** (review
  M8): each task records a digest of its resolved paths, so
  `inputs=lambda: {y: ... for y in YEARS}` reruns when `YEARS` changes
  (`remake why`: "inputs 3 → 2"). Older records have no digest: not a
  trigger, backfilled on the next run.
- **Task keys are canonical** (review M19): numpy scalars, enum members and
  str/int/float subclasses in a matrix become plain values at expansion, so
  keys survive the SLURM JSON round-trip and numpy 1 → 2. Rule code receives
  plain values. Records under the old key are found and re-keyed.
  **Breaking for enum matrices:** a rule body given `kind` from
  `matrix={'kind': list(Kind)}` now gets `kind.value`, not the member, so
  `kind.name` fails; use `Kind(kind)`. This is what SLURM jobs already
  received after the JSON round-trip, so local and SLURM runs now agree.
- **Scope checks look at values, after the remakefile has run** (review M1,
  M2): helpers defined below a rule are checked; a name called like a stdlib
  module or builtin (`site = 'CHIL'`) is checked by its value; project
  values (`DATA_DIR = Path(...)`) and your own modules are reported, while
  library modules and what they export (`from loguru import logger`) are
  environment. Warnings now come when the pipeline is finalized, not at
  decoration (rule-level `strict_scope=True` still errors at decoration too).
  Expect new `ScopeWarning`s for values that were silently untracked.
- **A `uses` key beats an enclosing-function variable of the same name**
  (field note 1): in a rule factory the closure used to win, so the code
  that ran was not the code that was tracked. A different value warns.
- **Schema versioning** (0.9.0 item 3): the DB records its schema version
  (`PRAGMA user_version`, major·1000 + minor). A DB from a newer major is
  refused with an upgrade hint; a newer minor still opens. A coming 0.8.6
  will refuse a DB written by 0.9 (0.8.5 and earlier don't check). Job specs and sidecars carry a format `"v"`.
- **Schema (additive, migrated in place):** `task.paths_digest`, and new
  tables `output_stat` and `run`. Nothing reruns on upgrade.
- `set-state --pending` keeps a task's measured resources (it records a
  pending state rather than deleting the row), as documented (review L15).
- `rss_method` is `sample-self`/`rusage-self` when child processes were
  reaped but their memory peak wasn't measured — a lower bound (review M18).
  Docs say child memory is the max, not the sum, of children.
- `MetadataManager.update_task()` gained `output_stats=None`; backends that
  don't accept it are not passed it. New optional backend methods:
  `restamp_codes`, `get_output_stats`, `record_run`/`get_run`.
- **Deprecated:** the built-in JASMIN SLURM defaults when no profile is
  selected (a warning; removed at 1.0 — use `--profile jasmin`).
- New dependency: `tomli` on Python < 3.11.

- `MetadataManager.update_task()` gained an optional `resources=None`
  keyword. Third-party backends overriding it need the extra keyword in their
  signature; nothing else changes.
- `Remake.task_info()` returns an extra `resources` dict (all-`None` when the
  task never ran or was never measured), and `TaskRecord` gained
  `wall_s`/`cpu_s`/`max_rss_bytes`/`rss_method`.
- **Schema (additive, migrated in place):** `task` gained `wall_s`, `cpu_s`,
  `max_rss_bytes` and `rss_method`. Existing DBs are migrated on open;
  pre-upgrade records read as "not measured" and **nothing reruns**. The
  planner never reads these columns, so they can never become a rerun
  trigger. `set-state` does not clear them: they describe the last actual
  execution, not the task's current state.

- A task with **no path link** to an upstream rule (a `depends_on` used only
  for ordering), or one reading an output several upstream tasks share,
  depends on *every* upstream task: any upstream rerun reruns it, any
  upstream failure skips it. Previously, with a shared matrix, this was
  element-wise by kwargs. After upgrading, such a task also reruns once if
  any upstream task last ran in a later invocation than it did.
- `run-all` runs each remakefile in its own process (`python -m remake`), so
  same-named helper modules in different project dirs no longer collide.
- A remakefile that fails to import (a missing package) is a clean error
  (exit 2) with the traceback at `-D`, not an uncaught traceback.
- `export-task -o` writes relative to where remake was invoked, like `dag -o`.
- `Ensure()` raises `TypeError` for an output it can't check (`ZarrStore`,
  `S3Object`, or any token without `validate()`), instead of silently doing
  nothing.
- numpy `datetime64`/`timedelta64` matrix values are still rejected (encode
  them as strings); only numpy numbers, bools and strings become plain values.
- `set-state --pending` keeps each record's run counter, so it doesn't make
  up-to-date downstream tasks look stale.

### Fixed

- The peak-RSS sampler could lose the peak as a task ended; stopping it is
  now deterministic (review L16).
- **Rerun propagation follows paths, not matrices** (review 2026-09-24 H3).
  Which upstream tasks a task depends on is now derived from its declared
  `inputs`: the upstream tasks whose outputs it reads. Previously a rule
  sharing its upstream's matrix was assumed to read only its same-kwargs
  upstream task, so a task reading upstream `year-1` (or a stencil of
  neighbours) did **not** rerun when that upstream task did — and the
  same-kwargs task reran instead. This applies to same-run propagation, the
  cross-run `run_seq` backstop, `set-state --success`'s downstream cascade,
  skipping downstream tasks after a failure, and `remake why` (which now
  names the upstream task read). Fan-ins across different matrices become
  precise too: `aggregate[site]` reruns only for the site whose inputs
  changed. SLURM's `aftercorr`/`afterok` choice uses the same map. After
  upgrading, tasks that were wrongly left looking up to date by the old rule
  are rerun; nothing else is.
- **Scope check understands PEP 810 lazy imports** (Python 3.15). A module
  bound by `lazy import numpy as np` was reported as an undeclared
  outer-scope name, because the scope check saw the unresolved lazy proxy
  rather than a module. Lazy imports are now classified without resolving
  them (so loading a remakefile still skips the import cost):
  `lazy import x[.y] [as z]` is environment like an eager module import, and
  `lazy from x import y` is environment only when `x` is stdlib, otherwise
  reported like an eager `from helpers import normalise`. One difference from
  eager imports: `lazy from pkg import submodule` is reported.

## [0.8.5] — 2026-09-24

The 0.8.5 lane: the process-handling fixes deferred from 0.8.4 (review IDs
refer to `design_docs/code_reviews/2026-09-24_review.md` on `main`).

### Fixed

- **A worker crash no longer aborts a multiproc or dask run** (H7). When a
  worker process died mid-task — killed for using too much memory, a
  segfault in native code, `os._exit()` — multiproc aborted the whole run
  with a `BrokenProcessPool` traceback, the crashing task was never recorded
  (so the next run crashed again), and the rest of the rule never ran; dask
  retried the crasher on three more workers (killing each) and re-executed
  tasks that had already finished. Now only the task that killed its worker
  is recorded as failed, with an explanation, and the run carries on: under
  multiproc, tasks in flight at the crash are re-run one at a time to find
  the culprit and tasks that never started are resubmitted; under dask
  every task executes at most once.
- **Ctrl-C and SIGTERM stop a multiproc or dask run promptly and cleanly**
  (M10). Ctrl-C let every queued task of the current rule run to
  completion before stopping, and SIGTERM (`kill`, a CI or scheduler
  timeout) killed remake but left its worker processes running — starting
  new tasks, and racing a later run on the same outputs. Now queued tasks
  are cancelled, workers are terminated, the run lock is released, and
  `remake run` prints `interrupted (SIGINT|SIGTERM)` and exits 130/143
  instead of a traceback.
- **The CLI closes the metadata database when a command ends**, instead of
  leaving the SQLite connection to garbage collection — the source of the
  `ResourceWarning: unclosed database` seen in the test suite (notably under
  dask on Python 3.14), and an open DB handle for in-process callers of
  `remake_cmd`.

## [0.8.4] — 2026-09-24

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

[0.10.0]: https://github.com/markmuetz/remake/releases/tag/v0.10.0
[0.9.0]: https://github.com/markmuetz/remake/releases/tag/v0.9.0
[0.8.5]: https://github.com/markmuetz/remake/releases/tag/v0.8.5
[0.8.4]: https://github.com/markmuetz/remake/releases/tag/v0.8.4
[0.8.3]: https://github.com/markmuetz/remake/releases/tag/v0.8.3
[0.8.2]: https://github.com/markmuetz/remake/releases/tag/v0.8.2
[0.8.1]: https://github.com/markmuetz/remake/releases/tag/v0.8.1
[0.8.0]: https://github.com/markmuetz/remake/releases/tag/v0.8.0
