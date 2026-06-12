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
- [ ] Downstream tasks of a failed task run anyway and fail "naturally" on
  missing inputs — correct in the DB but noisy and wasteful. Skip (and
  report) descendants of same-run failures.

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

- [ ] `executors/multiproc_executor.py` is stale remake2 code, unimportable
  by design. Port or delete; do not leave it to mislead readers.
  (`slurm_executor.py` rewritten 2026-06-12.)

## Smaller debts

- [ ] `eval`-based query filter (see MM comment in `core/planner.py`):
  consider a restricted-ops parser for better errors; revisit at CLI work.
- [ ] `uses` injection silently shadows module globals on name collision —
  warn at decoration time.
- [x] File logging was dropped in the CLI rewrite; restored: always-on DEBUG
  log at `.remake/remake.log` (rotated) for any remakefile subcommand.
- [ ] No Hypothesis property tests despite the design doc promising them
  (task key uniqueness/stability, matrix normalisation).
- [ ] `retry_lock_commit` concurrency machinery is carried over but
  untested in anger — test under real contention when multiproc lands.
- [ ] `ZarrStore.is_complete()` checks `.zmetadata` (zarr v2); zarr v3
  consolidated metadata lives in `zarr.json`. Handle both when xarray/zarr
  versions move.
