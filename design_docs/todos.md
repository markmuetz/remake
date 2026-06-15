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
