# Todos

Concrete known problems and debts, from the post-core-implementation
assessment (2026-06-11). Ordered roughly by severity.

## Performance / scaling

- [x] `Remake.task_from_key` materialised every task of every rule to find
  one key. Now: `task_from_spec(rule_name, kwargs)` constructs directly
  (0.04 ms at 1e6 tasks — the SLURM job-spec path), and `task_from_key`
  streams via `iter_tasks()` with early exit for full-length keys
  (worst case 3 s / constant memory at 1e6 tasks, vs ~8.5 s / 2.1 GB).
- [x] `Sqlite3Backend.get_tasks_status` was an N+1 query loop. Now batched
  `WHERE key IN (...)` in chunks of 900. plan() against a fully-populated
  1e6-row DB: 13.9 s (dominated by per-task rerun logic, not queries).
- [ ] Make the 1e6-task benchmark a load-bearing part of CI (script:
  `tests/benchmarks/bench_million_tasks.py`). Baseline measured 2026-06-11
  (in-memory DB, local ext4): load+finalize 0.001s (lazy goal achieved);
  expand+keys 4s / 0.5 GB; full materialisation 8.5s / 2.1 GB;
  plan(never) 7.5s — mostly the N+1 SELECT loop; plan(fallback, empty DB)
  22s — 1e6 stat calls, which on a parallel cluster filesystem could be
  minutes. fallback only pays this for tasks with no DB record, but the
  first plan of a restored pipeline does exactly that.
- [ ] Recording completions is one EXCLUSIVE transaction per task (1e6
  transactions over a big run) — batch or relax when addressing the bulk
  query.

## Correctness (cont.)

- [x] **SLURM JSON round-trip silently changes task identity for
  non-JSON-stable kwarg values.** Fixed 2026-06-16 with option (b), the
  guard: `iter_expand_rule` now rejects any matrix kwarg value that isn't a
  JSON scalar (str/int/float/bool/None) with a `SignatureError` naming the
  rule, the offending key/value/type, *why* (the JSON round-trip changes the
  key + kwarg-derived paths), and the fix (encode as a string). Fires at plan
  time before any SLURM submission, so bad specs are never written; scalar
  kwargs round-trip identically so existing keys/DBs are unaffected. Tests in
  test_dag.py. Chose the guard over (a) canonicalise-kwargs because list-valued
  paths (`[0, 20]`) are a smell and a loud failure beats a silent type change.
  Original report below.
- [x] (was) **SLURM JSON round-trip silently changes task identity for
  non-JSON-stable kwarg values.** Task kwargs are serialised to
  `.remake/jobs/<rule>.json` and reloaded on the compute node by
  `run-array-task`. JSON has no tuples, so a tuple (or nested tuple) kwarg
  value comes back as a list — and `Task.key` is `sha1(repr(dict(sorted(
  kwargs.items()))))`, so the reloaded key differs from the plan-time key.
  Consequences seen in the mcs_prime migration (2026-06-16): a `plot_kwargs`
  matrix value stored as `tuple(d.items())` (e.g. `(('xlim', (0, 20)),)`)
  ran fine locally but under SLURM every such task recorded its sidecar
  under a list-derived key the planner never looked up → all showed "never
  recorded in DB" despite succeeding; and any output path built from the
  kwargs (`xlim=(0,20)` → `xlim=[0,20]`) was written under the wrong name →
  planner saw it missing. Local executors are unaffected (no round-trip), so
  it only bites at scale on SLURM. Options: (a) **canonicalise kwargs
  through JSON before computing `Task.key`** (so plan-time and reload-time
  agree by construction — simplest, makes keys round-trip-invariant); and/or
  (b) **reject non-JSON-stable kwarg values at plan time** with a clear error
  (tuples, sets, nested non-scalars), steering users to str/int/bool/scalar
  matrix values. (a) is the real fix; (b) is a cheap guard. Workaround used
  downstream: encode the dict as a canonical string matrix value.

## Failure UX

- [x] `run_task` stores `repr(e)` — the traceback is lost (remake2 stored
  `traceback.format_exc()`). Now stores `traceback.format_exc()`.
- [x] No way to see failures from the CLI: now `info --show-failures` prints
  each failed task with its stored traceback and failure timestamp.
- [x] Downstream tasks of a failed task run anyway and fail "naturally" on
  missing inputs — correct in the DB but noisy and wasteful. Fixed
  2026-06-12: singleproc/multiproc skip (and report) tasks tainted by
  same-run failures — element-wise when matrices are shared, conservative
  otherwise (`planner.upstream_failed`, mirroring rerun propagation).
  Skipped tasks stay unrecorded (pending), so fixing the upstream makes
  the next run pick them up. SLURM gets this from aftercorr/afterok.

## Packaging

- [x] `pip install -e .` has never been run against the new pyproject; the
  dynamic `version = {attr=...}` may import the package (and deps) at build
  time. Now: `version.py` is a static literal; `uv build` and an editable
  install in a uv-managed venv both verified (2026-06-12).
- [x] The installed `remake` console script has only been tested as
  `python -m remake.remake_cmd`. Now verified via `uv run remake version`.
- [x] Version string: `__version__` is `0.8.0`; the decided version
  "0.8.0.0-alpha" is not valid PEP 440 anyway. Now `0.8.0a0`.

## Dead code

- [x] `executors/multiproc_executor.py` is stale remake2 code, unimportable
  by design. Port or delete; do not leave it to mislead readers.
  (`slurm_executor.py` rewritten 2026-06-12; `multiproc_executor.py`
  rewritten 2026-06-12: spawned workers reload the remakefile, record via
  sidecars, per-rule barriers.)

## Correctness

- [x] **Planner ignores `inputs=`/`outputs=` changes — only `run` code and
  `uses` invalidate a task.** Fixed 2026-06-16 (commit io_hash): added
  `scope.io_hash(rule)` (AST-normalised `inputs`+`outputs` source, mirroring
  `uses_hash`), persisted as a new `io_hash TEXT` column on `task` (with a
  defensive `ALTER TABLE` migration so in-flight DBs upgrade without a delete;
  pre-upgrade NULL rows are treated as not-yet-tracked, no mass rerun). The
  planner reruns when the stored `io_hash` differs; `explain_task`/`why`
  surface an `io-changed` reason. Caveat acknowledged: a pure output-*path*
  change driven by a captured constant (not in the spec source) is the
  closure case — won't be caught unless threaded through the matrix or
  declared in `uses` (documented in the rules-and-tasks lambda/closure note).
  Original report below.
  - (was) `task_will_run` compared only `run_code` and `uses_hash`, never the
    inputs/outputs spec, so editing the inputs/outputs dict or function did
    not rerun an already-succeeded task. Surfaced in the mcs_prime remake2→3
    migration (2026-06-16): redirecting an output directory left outputs
    orphaned at the old path while downstream looked at the new one.
    Workaround used there: fresh `.remake/` + empty output tree.

## Smaller debts

- [ ] `RemakeError` prints a full traceback instead of a clean message:
  `remake_cmd` has no top-level `except RemakeError`, so user-facing errors
  (e.g. a `task-info`/`task-log` `-Q` query matching >1 task) surface as
  tracebacks. Catch `RemakeError` at the top, print `error: {e}` and
  `sys.exit(2)`; keep the traceback only under `-X`/`--debug-exception`.
  Found during the slurm-status/why/lint JASMIN validation 2026-06-15.
  Note: the `why` >1-match case that first surfaced this is gone — `why`
  now explains all matches (2026-06-15) — but the general missing-handler
  problem remains for the other single-task commands and bad queries.
- [ ] `eval`-based query filter (see MM comment in `core/planner.py`):
  consider a restricted-ops parser for better errors; revisit at CLI work.
- [ ] `uses` injection silently shadows module globals on name collision —
  warn at decoration time.
- [x] File logging was dropped in the CLI rewrite; restored: always-on DEBUG
  log at `.remake/remake.log` (rotated) for any remakefile subcommand.
- [x] `.remake/remake.log` is a single shared file; under a wide SLURM array
  job each task process appends to it concurrently and lines interleave/
  corrupt (observed on JASMIN 2026-06-12, 176-element array). Fixed:
  `run-task`/`run-array-task` write a per-task log at
  `.remake/tasks/log/<rule>/<key[:2]>/<key[2:]>.log` instead of the shared
  sink — see design_docs/per_task_logging.md (incl. the open total-file-
  count budget question).
- [ ] No Hypothesis property tests despite the design doc promising them
  (task key uniqueness/stability, matrix normalisation).
- [ ] `retry_lock_commit` concurrency machinery has a sharp livelock cliff
  well below 400-way concurrency. Stress-tested on JASMIN 2026-06-12 with
  `tests/benchmarks/bench_sqlite_contention.py` — raw SLURM array jobs
  hammering `Sqlite3Backend.update_task` directly (no Remake pipeline),
  against `.remake/remake.db` on NFSv3:
    - 176-way (ex4, real compute between writes, naturally staggered):
      zero lock errors.
    - 400-way (tight loop, 50-150 `update_task` calls/task, no stagger):
      598 "database is locked" errors in 5 min; only 3/400 tasks completed
      at all (635 rows written total).
    - 800-way: only 2/800 tasks completed (294 rows total) — *less*
      aggregate throughput than 400-way. Past the cliff, more contenders
      means less total progress, not more.
  Not a graceful slowdown: past the cliff most processes never acquire the
  lock within a job's wall-time.
  **Fixed 2026-06-12 via sidecar/ingest** (design in
  slurm_implementation.md): `run-array-task` no longer opens the DB at
  all — results go to `.remake/tasks/results/...` sidecars, batch-ingested
  by the next plan/run/info. Validated at 400/800-way through the real
  pipeline path on JASMIN 2026-06-13 (`tests/benchmarks/bench_slurm_pipeline.py`
  setup/submit/report): both PASS, zero lock-marker lines, all sidecars
  ingested; `time-ingest` measured the single-writer ingest at ~2.5
  ms/sidecar, linear (800 in ~2.0 s, 1600 in ~3.9 s), no cliff.
  `retry_lock_commit` still guards the (now low-concurrency)
  ensure_rules/update_task/ingest paths.
- [ ] `ZarrStore.is_complete()` checks `.zmetadata` (zarr v2); zarr v3
  consolidated metadata lives in `zarr.json`. Handle both when xarray/zarr
  versions move.
- [x] Can `uses` take a class? (instead of a function) Yes: classes are
  callable, so they take the function path — whole class body hashed
  AST-normalised, injected like any value. Two fixes made it true in
  practice (2026-06-12): `load_module` registers in `sys.modules` (class
  getsource needs it; functions didn't) and the sourceless fallback no
  longer assumes `__code__` (classes lack one — was a crash). Caveats:
  inherited methods live in the base class, which must be declared in
  `uses` itself to be tracked (same one-level-deep rule as functions);
  REPL/exec-defined classes fall back to repr (body changes undetected).
- [x] `remake -X run` not launching pdb/ipdb on Exception. Two causes,
  fixed 2026-06-12: `exception_info` used `debug.pm()`, which needs
  `sys.last_traceback` (only set by the interactive interpreter) — now
  `post_mortem(tb)`; and executors swallow task failures by design, so
  nothing ever reached the excepthook — `-X` now sets
  `executor.raise_on_failure` so the first failure propagates with the
  original traceback (in-process executors only).
- [x] Check that failed SLURM jobs cause dependent jobs to not run. Tested on
  JASMIN 2026-06-14 (`tests/benchmarks/bench_slurm_failure.py`: stage1 fails a
  subset, stage2 shares its matrix 1-to-1 → aftercorr). Found a real bug: the
  per-rule sbatch wrapper ended with `echo "SLURM COMPLETED ..."`, whose exit 0
  masked the task's real exit code, so SLURM saw *every* element as COMPLETED
  0:0 and aftercorr/afterok never blocked anything (deliberate-fail elements +
  their dependants both ran-and-failed). Fixed: wrapper now `rc=$?; echo ...;
  exit $rc`, plus `#SBATCH --kill-on-invalid-dep=yes` so blocked dependants are
  cancelled (not parked PD forever). Re-run PASS: 10/40 stage1 FAILED 1:0,
  their 10 stage2 dependants never ran (absent from sacct, left pending), the
  other 30 ran clean; 0 stage2 ran-and-failed. Regression assertions added to
  `tests/integration/test_slurm.py`.
- [x] Move `-X` from `remake -X run` to `remake run -X`, and have it run the
  task(s) in-process directly so failures reach the debugger with the
  original traceback. (`-X` was a global flag because it was occasionally
  useful for debugging remake itself; trusting that's no longer needed.)
  Done 2026-06-13: `--debug-exception`/`-X` is now a `run` subcommand flag;
  when set it forces the singleproc executor (warning if `-E` was something
  else) so the failure propagates in-process into pdb/ipdb. See the rejected
  orchestrator-daemon entry in discussion.md.
## SLURM

- [ ] `--dry-run` overwrites `.remake/jobs/<rule>.json` without checking
  `squeue` for in-flight jobs. A real `run` guards against this (per-rule
  `_queued_jobids` check skips the rule if any element is still PD/R), but
  `--dry-run` bypasses that guard because it never enters the submit path.
  Consequence: if a user modifies the remakefile (changing the matrix) and
  does a `--dry-run` while array jobs from a previous submission are still
  running, the spec files are overwritten with the new task mapping and
  `run-array-task` looks up the wrong task for a given
  `SLURM_ARRAY_TASK_ID`. Fix: apply the same `_queued_jobids` skip (or at
  least don't overwrite the spec file) during `--dry-run`.
- [ ] Per-task "already running?" detection. Rule-level skipping exists
  (`squeue_snapshot`/`_active_jobids`/`_queued_jobids` skip a whole rule whose
  last submission is still PD/R); make it per-task and replan-proof by stamping
  each job with a run id + its spec path so a queue snapshot maps back to the
  exact remake task. Also fixes the latent resubmit-all bug where a *failed*
  squeue is read as an empty queue. Design: [slurm_already_running.md](slurm_already_running.md).

## UX

- [x] Debug/trace logging for visibility into what's happening. Added a new
  `-T`/`--trace` level (loguru TRACE, below DEBUG); convention is DEBUG
  summarises loops (counts/timings), TRACE logs each element — using loguru's
  lazy `{}` formatting so trace lines cost nothing until a sink accepts them
  (convention documented in per_task_logging.md). Seams: `planner.plan` (silent
  before — per-rule + overall summaries at debug, per-task rerun reason at
  trace), `remake.run` wave loop, `get_tasks_status`, `ensure_rules`,
  `ingest_sidecars`, SLURM `run_tasks`.
- [x] `remake info`: totals row at the bottom (text + `--json` `totals` key),
  rules iterated in dependency (topological) order rather than declaration
  order.
- [x] `remake run -E slurm`: prints one line per rule submitted with its task
  count and kind (e.g. "extract: submitting 1234 task(s) (array)"). Still the
  natural home for the "skipped N already-queued tasks" message once the
  per-task SLURM guard (see SLURM section) lands.
- [ ] Surface `io_hash` (and `uses_hash`/run-code) recorded-vs-current on the
  "inputs/outputs spec changed" / "uses= changed" / "run code changed" verdicts.
  `why` names the category but not *what* differs, and `task-info` doesn't expose
  the stored hashes at all — so diagnosing a spec-change rerun means reading
  `.remake/remake.db` directly and diffing the AST-normalised segments by hand.
  Motivating case (wescon-tools, 2026-06-17): 5 of 1465 `compare` tasks reran on
  "inputs/outputs spec changed"; the only way to find out why was to pull the
  recorded `io_hash` from the DB and diff its `inputs=`/`outputs=` segments
  against `scope.io_hash(rule)` — which revealed the 5 carried a malformed
  `io_hash` (the `inputs` segment held the `outputs` AST) recorded by an earlier
  build. Proposal: `why`/`task-info` (esp. `--json`) should show recorded vs
  current for the relevant hash, and ideally a segment-level diff for io_hash
  (`inputs=`/`outputs=`), so this needs no DB spelunking.
- [ ] Calling e.g. `remake run path/to/remakefile.py` should cd into the 
  remakefile's directory first, so that you don't create a new .remake directory
  in cwd.
- [ ] "AST" is too technical for user-facing material. Reserve the term for
  internal docs/code and a deep-dive "how the planner decides" technical
  section; everywhere a user reads it say "code structure" (or "the code,
  compared by structure"), with "(AST)" at most in parentheses on first
  mention. The `why`/`info --reasons` reason strings are already clean
  ("run code changed since last run"); the planner.py "AST" mentions are
  internal docstrings/comments (fine to keep). Done in examples/README and
  ex3 (2026-06-19); still to do: `docs/guide/rules-and-tasks.md` (the one
  user-facing doc that uses the term).
