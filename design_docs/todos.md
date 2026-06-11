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

- [ ] `run_task` stores `repr(e)` — the traceback is lost (remake2 stored
  `traceback.format_exc()`). Store the full traceback.
- [ ] No way to see failures from the CLI: add `info --show-failures` (or
  similar). During example debugging the DB had to be queried by hand.
- [ ] Downstream tasks of a failed task run anyway and fail "naturally" on
  missing inputs — correct in the DB but noisy and wasteful. Skip (and
  report) descendants of same-run failures.

## Packaging

- [ ] `pip install -e .` has never been run against the new pyproject; the
  dynamic `version = {attr=...}` may import the package (and deps) at build
  time. Verify, and make `version.py` statically readable if needed.
- [ ] The installed `remake` console script has only been tested as
  `python -m remake.remake_cmd`.
- [ ] Version string: `__version__` is `0.8.0`; the decided version
  "0.8.0.0-alpha" is not valid PEP 440 anyway. Settle on `0.8.0a0`-style.

## Dead code

- [ ] `executors/multiproc_executor.py` and `executors/slurm_executor.py`
  are stale remake2 code, unimportable by design. Port or delete; do not
  leave them to mislead readers.

## Smaller debts

- [ ] `eval`-based query filter (see MM comment in `core/planner.py`):
  consider a restricted-ops parser for better errors; revisit at CLI work.
- [ ] `uses` injection silently shadows module globals on name collision —
  warn at decoration time.
- [ ] File logging was dropped in the CLI rewrite; restore it (matters on
  clusters) when SLURM lands.
- [ ] No Hypothesis property tests despite the design doc promising them
  (task key uniqueness/stability, matrix normalisation).
- [ ] `retry_lock_commit` concurrency machinery is carried over but
  untested in anger — test under real contention when multiproc lands.
- [ ] `ZarrStore.is_complete()` checks `.zmetadata` (zarr v2); zarr v3
  consolidated metadata lives in `zarr.json`. Handle both when xarray/zarr
  versions move.
