# Todos

Concrete known problems and debts, from the post-core-implementation
assessment (2026-06-11). Ordered roughly by severity.

## Performance / scaling

- [ ] `Remake.task_from_key` materialises every task of every rule to find
  one key — and `remake run-task` (the SLURM array hot path) calls it.
  Replace with direct construction: the per-rule SLURM JSON already carries
  `rule` + `kwargs`, so lookup should not be a search.
- [ ] `Sqlite3Backend.get_tasks_status` is an N+1 query loop (one SELECT per
  task, every plan). Bulk-query (`WHERE key IN (...)` batches, or remake2's
  backup-to-`:memory:` trick for reads).
- [ ] The 1e6-task design claim is unbenchmarked — nothing beyond ~530 tasks
  has been exercised. Add a synthetic 1e5+ task benchmark test so the
  scaling claim is load-bearing.

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
