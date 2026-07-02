# Todos

Concrete known problems and debts, from the post-core-implementation
assessment (2026-06-11). Ordered roughly by severity.

## Performance / scaling

> **Scale target (MM, 2026-07-02; see remake3_design.md "Scale target"):
> ~1e4 tasks × ~1e2 files/task = ~1e6 FILES — not 1e6 tasks.** The 1e6-task
> figure in older items below is stress headroom (and impossible as one
> SLURM array anyway), not the requirement. Per-task costs are trivial at
> 1e4 post the 2026-07 rework; the scaling frontier is per-FILE work
> (token resolution, stat sweeps). Benchmark new work against
> `tests/benchmarks/bench_field_scale.py` (1e4 tasks × 100 outputs;
> baseline 2026-07-02: resolve 1e6 tokens 2.1 s / 0.63 GB, fallback stat
> sweep 2.3 s on local ext4 — the stat number is the best case, Lustre/NFS
> is 10–100× that).

- [ ] **File-side scaling frontier: 1e6-path stat sweeps on parallel
  filesystems.** `check_outputs` (`fallback`/`always`) and `--check`-style
  paths stat every declared output — fine locally (2.3 s), minutes on
  Lustre/NFS where each stat is a round trip. When this bites in the field:
  consider directory-listing instead of per-file stat (one `scandir` per
  output dir covers all files in it), batching/parallelising the sweep, or
  a per-rule completion sentinel. Needs a real cluster measurement first —
  don't engineer ahead of data.
- [x] `Remake.task_from_key` materialised every task of every rule to find
  one key. Now: `task_from_spec(rule_name, kwargs)` constructs directly
  (0.04 ms at 1e6 tasks — the SLURM job-spec path), and `task_from_key`
  streams via `iter_tasks()` with early exit for full-length keys
  (worst case 3 s / constant memory at 1e6 tasks, vs ~8.5 s / 2.1 GB).
- [x] `Sqlite3Backend.get_tasks_status` was an N+1 query loop. Now batched
  `WHERE key IN (...)` in chunks of 900. plan() against a fully-populated
  1e6-row DB: 13.9 s (dominated by per-task rerun logic, not queries).
- [ ] Make the scale benchmarks load-bearing in CI. **Reframed 2026-07-02:**
  the one that should gate is `bench_field_scale.py` (the design shape,
  ~5 s total — cheap enough to run as a test with generous thresholds);
  `bench_million_tasks.py` stays as the manual stress-headroom check.
  Original 1e6-task item below.
  Baseline measured 2026-06-11
  (in-memory DB, local ext4): load+finalize 0.001s (lazy goal achieved);
  expand+keys 4s / 0.5 GB; full materialisation 8.5s / 2.1 GB;
  plan(never) 7.5s — mostly the N+1 SELECT loop; plan(fallback, empty DB)
  22s — 1e6 stat calls, which on a parallel cluster filesystem could be
  minutes. fallback only pays this for tasks with no DB record, but the
  first plan of a restored pipeline does exactly that.
- [ ] Recording completions is one EXCLUSIVE transaction per task (1e6
  transactions over a big run) — batch or relax when addressing the bulk
  query. **Deprioritised 2026-07-02 per the scale target:** at the design
  scale (1e4 tasks) this is seconds over a whole run, and bulk paths
  (set-state, ingest) already batch into one transaction
  (bench_field_scale: 1e4 completions in 0.08 s). Only worth revisiting
  if per-task commit latency shows up on NFS in the field.
- [x] **Hash-first code-change detection: stop hauling full source per task;
  move `uses`/`io` source into the content-addressed `code` table.** Done
  2026-07-02, with one design change from the plan below: instead of a sha1
  digest per task, `task.uses_hash`/`io_hash` became **integer FKs**
  (`uses_code_id`/`io_code_id`) to the normalised strings interned
  (find-or-insert by exact content) in `code` — smaller than a digest, and
  the old rendering stays recoverable by id, so `why` keeps its before→after
  uses messages. `get_tasks_status` no longer JOINs `code.code` (rows carry
  only ints); the planner resolves the distinct few ids per rule via the new
  `get_codes` and compares once per id, set-membership per task. Current
  uses/io ids are interned once per rule in `_ensure_rule` (also kills the
  per-task `compute_uses_hash` — was 1e6 AST renders on a big run), and rule
  run/inputs/outputs inserts now intern too (an edit-and-revert maps back to
  the original row instead of rerunning everything). In-place migration
  `_migrate_inline_hashes_to_code_ids` backfills FKs from the old inline
  columns (io_hash NULL survives as not-tracked), drops them (NULLs on
  SQLite <3.35) and VACUUMs. Tests: migration round-trip incl. no-mass-rerun
  and uses-change rerun (test_metadata.py); verified end-to-end via CLI
  (run/info/why incl. `threshold: 5 → 7` message, and a hand-built old-schema
  DB migrated by `remake info` with zero reruns). Bug 04 Issue 2's
  hypothesis confirmed: the status SELECT did pull `uses_hash` per row.
  Remaining stage B below. Original plan:
  - *Query side (logs_analysis §1.1/1.2/5).* `get_tasks_status`
    (`sqlite3_backend.py:248`) JOINs `code.code` and returns the **full run
    source for every task**, which the planner feeds to `CodeComparer` — so
    status-query cost scales with previously-run tasks. Field measurement
    (wescon-tools, 3341 tasks): 38.4 MB fetched per query vs 149 KB distinct
    code — **256× amplification**; p99 3.6 s, max 10.2 s per call; 363 s total
    across the mined logs; planner runs up to 20 s to conclude "0 runnable".
    Fix: store/compare a `code_hash` (of the normalised AST) and fetch full
    source lazily by `run_code_id` only when the hash differs and a diff is
    rendered.
  - *Storage side ("Display code changes in `uses` functions",
    [discussion.md](discussion.md)).* Today `task.uses_hash`/`io_hash` store
    the full AST-normalised source string *inline on every task row* (misnomer:
    it's a serialised AST, not a digest) — a verbose blob duplicated across
    every task of a rule. Measured in the wild: 272 MB DB for 3341 tasks,
    99.8% of it `task.uses_hash` (~79 KB/row, distinct=1 per rule). `run`/
    `inputs`/`outputs` already do this right: raw source deduped in `code`,
    FK from `rule`. Do the same for `uses` (needs a `rule_uses(rule_id, name,
    code_id, kind)` join table — `uses` is a dict of N helpers, one row each;
    `kind` distinguishes sourceable functions from plain values / sourceless
    callables, since only the first is diffable) and `io`, and demote the
    per-task columns to an actual `sha1` of the name-sorted normalised AST.
  Keeps change detection exact, shrinks task rows and query payloads, and
  stores raw source once per rule — unlocking the human-readable `uses`
  code-change diff and `rule-info` source display. Schema migration required
  (alpha 0.8.0a0, acceptable). Likely also resolves
  [bug 04](bugs/04_info_redundant_and_superlinear_status_queries.md) Issue 2
  (superlinear `get_tasks_status`: the slowest rule's `uses_hash` is
  ~145 KB/row × 1465 tasks ≈ 213 MB scanned per status query) — confirm
  whether the status SELECT pulls `uses_hash` before profiling further.
- [x] **Stage B of the storage rework: per-helper raw-source table.** Done
  2026-07-02, keyed differently from the sketched design: `uses_manifest(
  uses_code_id, name, code_id, kind)` hangs off the *uses version* (the
  joined-string code id tasks point at), not `rule_id` — rule-keyed rows
  would be overwritten to current at ensure time, so `why` could never
  diff against what a task actually ran with; version-keyed manifests are
  write-once/immutable and any stored `uses_code_id` resolves to its
  helpers forever. One row per helper, raw rendering interned in `code`
  (kind: source/value/bytecode via `scope.raw_uses_parts`), so editing one
  of N helpers shares the other N-1's rows. `why`'s uses-changed message
  now shows a real unified diff of helper source (`(body):` + diff),
  before→after for values (incl. multi-line reprs), "(body; source
  unavailable)" for sourceless callables, and degrades to bare "(body)"
  for records predating the table (no backfill possible — old raw sources
  are gone). Migration: CREATE TABLE on existing DBs. `rule-info` source
  display (the remaining consumer) can read the manifest or live objects
  when that command lands. Verified via CLI: body edit → diff, combined
  body+value change, manifest sharing across versions.
- [x] **`VACUUM` the field DBs** (logs_analysis §1.5) — folded into the
  migration above: `_migrate_inline_hashes_to_code_ids` VACUUMs after
  dropping the inline columns, so existing bloated DBs (272 MB wescon-tools)
  recover the space on first contact with the new code.
- [x] **`remake info` queries every rule's status twice**
  ([bug 04](bugs/04_info_redundant_and_superlinear_status_queries.md)
  Issue 1). **Done 2026-07-02** via `RecordCache` (metadata_manager.py): a
  per-invocation read-through cache (per task key, misses cached too)
  wrapped around the backend for the read-only commands, so the plan pass
  warms it and the renderer/explainers hit it — each record fetched at most
  once per invocation. The audit found `why -Q` had a worse variant (the
  durable-propagation check re-queried each upstream rule's full record set
  once per explained task, N×M) — same cache fixes it; `info --reasons`
  likewise. `run`'s wave loop and `set-state` stay uncached on purpose
  (their records change mid-invocation). Spy tests assert each key is
  fetched exactly once (test_planner.py); both fail against the pre-fix
  code.
- [x] **Status-query regression micro-benchmark** (logs_analysis §4.5). Done
  2026-07-02: `test_status_query_time_independent_of_uses_size`
  (test_metadata.py) — pytest, so CI-load-bearing now, not a manual bench
  script. Asserts the scaling *ratio* rather than a wall clock (CI-noise
  robust): 2000 tasks with a wescon-sized (~150 KB rendered) `uses` must
  query and plan within 5× of the same DAG with a tiny `uses` (healthy ≈ 1×;
  the pre-rework JOIN was ~100×), with a 0.05 s timer floor. Plus a
  timer-free structural canary (records carry int ids, never text) and a
  semantic guard (nothing planned as a rerun). Validated by simulating the
  regression (re-adding a per-task text fetch): fails with a pointed message.
  Runs in ~0.5 s.

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
- [x] rule plumbing errors should be treated differently from individual task
  errors. **Done 2026-07-02:** `rule.check_io_spec` validates inputs/outputs
  specs against the matrix keys — a callable spec's required parameters must
  all be matrix keys (it's called with the kwargs it names), and a dict
  spec's template fields (`'{n}'`, incl. format specs / attribute access)
  must be matrix keys. Raises `SignatureError` once, naming the rule/part/
  missing names and saying it would fail every task identically — at
  decoration for static matrices, at first expansion for callable ones
  (same split as the run-fn check). Surfaces as a clean one-line `error:`
  via the CLI. Tests in test_rule_signature.py.
- [x] Just as there is a `remake task-info`, there should be a `remake rule-info`.
  **Done 2026-07-02.** Docstring at top (cleandoc'd), depends-on/dependents,
  matrix (keys/values/task count; dynamic matrices report "not resolvable
  yet"), input/output *templates*, uses (helper source shown in full, values
  as `name = repr`, sourceless as "source unavailable"), config; `--json`.
  Templates from callables use exactly the suggested trick (call with
  `case='{case}'`) but via a placeholder *object* that only supports
  rendering — a plain string silently produced a wrong template when the
  callable computed with the kwarg (`n * 2` → 'in/{n}{n}.txt'), so any
  non-formatting use now reports "templates not derivable" instead.
  `Remake.rule_info()` is static introspection only (fresh DAG, no metadata).
  Tests in test_cli.py.

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
- [ ] `eval`-based query filter (see MM comment in `core/planner.py:27`):
  `make_predicate` does `eval(compile(query, ...))` against task kwargs.
  Hardened (`__builtins__` stripped, kwargs as the only locals) and the query
  is the user's own, so the threat model is low — but it is not a sandbox
  (dunder traversal on passed objects, resource exhaustion). Replace with a
  restricted-ops parser that walks the AST and allow-lists a small set of
  comparisons/boolean ops (the pyquerylist approach), which also yields better
  error messages than a bare `NameError`/`SyntaxError`. Pre-1.0 fix; revisit
  at CLI work.
- [x] **`planner.plan` records only the first rerun trigger** (MM comment).
  **Done 2026-07-02**, with a finding: `why`/`info --reasons` were *already*
  full-fidelity — they use `explain_task`, whose checks are independent
  `if`s that report every applicable reason. The single-reason gap was only
  plan()'s own per-task reason (the TRACE line). Now the cheap trio
  (run-code/uses/io — three int set-memberships) is checked jointly and
  joined ("uses= changed + inputs/outputs spec changed"); the expensive
  checks (check_outputs stat calls, upstream scan) stay deliberately
  short-circuited — explain_task remains the full view. Tests in
  test_planner.py.
- [x] **`planner.plan` does redundant work when `force` is set** (MM comment).
  **Done 2026-07-02.** `if force:` is now the first branch of the task loop
  (skips freshness checks and check_outputs stat calls), and the per-rule
  comparison prep (source rendering — `uses_hash` alone can be ~100 KB —
  plus `get_codes` and the CodeComparer set-building) is skipped entirely
  under `force` *or* `ignore_code_changes`, since nothing reads the sets.
  Behaviour-preserving; covered by existing force/-I tests.
- [ ] **`planner.plan` is long and interleaves three concerns** (MM comments,
  `core/planner.py:278-279`): matrix-deferral, per-task freshness, and rerun
  propagation (the same-pass `rerun_kwargs` path *and* the durable `run_seq`
  backstop). Extract the per-task decision (the body of `for task in tasks`)
  into a `_task_rerun(task, rec, ...) -> (rerun, reason)` helper so `plan` reads
  as orchestration. Behaviour-preserving readability refactor; well covered by
  the planner tests.
- [x] `uses` injection silently shadows module globals on name collision —
  warn at decoration time. **Done 2026-07-02:** `scope.check_shadowing`,
  called by the `@rule` decorator — warns (`ScopeWarning`) when a uses key
  matches a module global bound to a *different* object (identity first,
  then equality, so the standard `uses={'helper': helper}` tracking idiom
  and re-typed equal literals stay silent). Tests in test_scope.py.
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

- [x] `--dry-run` overwrites `.remake/jobs/<rule>.json` without checking
  `squeue` for in-flight jobs. **Stale — already guarded** (verified
  2026-07-02): the `_queued_jobids` skip runs at the top of
  `SlurmExecutor.run_tasks`, *before* spec-writing, and `--dry-run` goes
  through the same path (`dry_run` only short-circuits the final
  `submit()`), so a queued rule's specs are never rewritten by a dry run.
  Locked in with a regression test
  (`test_dry_run_does_not_overwrite_queued_rule_specs`). Original report
  below.
  - (was) A real `run` guards against this but `--dry-run` was thought to
    bypass the guard; consequence would have been spec files overwritten
    with a new task mapping while `run-array-task` elements still read them.
- [ ] Per-task "already running?" detection. Rule-level skipping exists
  (`squeue_snapshot`/`_active_jobids`/`_queued_jobids` skip a whole rule whose
  last submission is still PD/R); make it per-task and replan-proof by stamping
  each job with a run id + its spec path so a queue snapshot maps back to the
  exact remake task. Also fixes the latent resubmit-all bug where a *failed*
  squeue is read as an empty queue. Design: [slurm_already_running.md](slurm_already_running.md).
- [ ] Check behaviour of deferrable rules under SLURM. When running, I think 
  that the downstream tasks rerunning should have triggered a rerun of the 
  deferrable jobs but did not. Worth checking. 

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
- [x] Surface `io_hash` (and `uses_hash`/run-code) recorded-vs-current on the
  "inputs/outputs spec changed" / "uses= changed" / "run code changed" verdicts.
  **Done across 2026-07-02:** `why` now shows a unified source diff for
  run-code changes (pre-existing), per-helper raw-source/value diffs for
  uses changes (stage B manifest), and — closing this item — names which
  `io_hash` segment differs ("inputs and outputs segment(s) differ") via
  `scope.parse_io_hash`, which is exactly the diagnostic the wescon
  malformed-io_hash case needed (its `inputs` segment held the `outputs`
  AST). Not done: exposing raw stored-vs-current hashes in `task-info
  --json` — revisit if a case needs more than `why` now gives. Original
  motivating case below.
  - (was, wescon-tools 2026-06-17) 5 of 1465 `compare` tasks reran on
    "inputs/outputs spec changed"; finding out why meant pulling the
    recorded `io_hash` from the DB and diffing its segments by hand.
- [x] Calling e.g. `remake run path/to/remakefile.py` should cd into the
  remakefile's directory first, so that you don't create a new .remake directory
  in cwd. **Done 2026-06-22.** `remake_cmd` cds into the remakefile's parent for
  any subcommand with a `remakefile` arg, rewrites the arg to its bare name, and
  restores cwd after dispatch (so in-process/library callers don't see cwd
  move). `.remake/`, the pipeline's relative outputs, and the file log now
  anchor to the remakefile; the SLURM scripts re-invoke `run-array-task` with
  the bare name from the submit dir, so they stay consistent. Test:
  test_cli.py::test_run_cds_into_remakefile_directory.
- [x] "AST" is too technical for user-facing material. Reserve the term for
  internal docs/code and a deep-dive "how the planner decides" technical
  section; everywhere a user reads it say "code structure" (or "the code,
  compared by structure"), with "(AST)" at most in parentheses on first
  mention. **Done 2026-06-19.** Fixed in examples/README (parenthetical-on-
  first-mention form), ex3, and `docs/guide/rules-and-tasks.md`. The
  `why`/`info --reasons` reason strings were already clean ("run code changed
  since last run"); the remaining "AST" mentions are internal only
  (`core/scope.py`, `util/code_compare.py`, `core/planner.py` docstrings/
  comments) and are fine to keep.
- [x] Execution observability: per-task timing + a run summary. **Main asks
  done 2026-07-02**, all in `run_task`/`run()` so every executor gets them
  uniformly:
  1. **Per-task duration** — `run_task` times execution; completion at DEBUG
     ("completed proc[n=3] in 1.24s") and failure now carries elapsed too.
     Both are structured events (`task_complete`/`task_failed` with
     `seconds`/`rule`/`key` in remake.jsonl) — the raw material for the
     resource-advice / stats-store ideas is now being collected.
  2. **Run summary at INFO** — "ran 100 task(s), 0 failed in 42.3s"
     (`run_summary` event); the in-process path also says "Nothing to do"
     instead of ending silently.
  3. **`run_task` I/O-path line** — TRACE "running {task}: inputs [...] ->
     outputs [...]", `opt(lazy=True)` so the path lists cost nothing without
     a TRACE sink.
  - Remaining (lesser): log chosen executor/nproc/config at run start; the
    direct-DB-write-vs-sidecar decision and per-task `update_task` are
    unlogged (trace would suffice).
- [x] **Don't TRACE-log full function bodies in `code_compare`**
  (logs_analysis §3.1). Done 2026-07-02: the full-body dump (was ~55k of the
  56.8k lines in the worst field log) is now gated behind `REMAKE_LOG_CODE=1`;
  by default TRACE gets a one-line verdict per comparison
  (`code_compare: identical` / `unchanged (AST-equal)` / `changed`, with a
  `(cached)` variant). Bonus: the old lines built their strings by `+`-concat
  before loguru's lazy check, so the dump cost was paid even with no TRACE
  sink attached — the gated form uses lazy `{}` args.
- [x] **Split the file log into human + debug streams; threshold the timing
  lines** (logs_analysis §3.2/3.3). **Done 2026-07-02.** `remake.log` is now
  the INFO+ narrative; the DEBUG (TRACE under `-T`) firehose goes to a
  separate rotated `remake.debug.log` (each 5 MB × 3, so the streams no
  longer compete for one window). The `get_tasks_status ... in Xs` line
  reaches DEBUG only when >100 ms; fast queries are TRACE. All three shared
  sinks stay off in per-task processes (NFS concurrent-append rule).
- [x] **Structured logging for mineability** (logs_analysis §4). **Done
  2026-07-02.** `.remake/remake.jsonl` (rotated, serialize=True) mirrors the
  debug stream as one JSON object per record; metrics are bound `extra`
  fields with stable `event=` tags (`invocation`, `ensure_rules`, `plan`,
  `status_query`, `ingest`, `wave`, `task_failed`), and every record from
  one invocation shares a `run_id` (bound via logger.configure per CLI
  call) — mining is `jq` on `.record.extra`, not regex. Example in
  docs/guide/debugging.md. Not done from §4: the in-tree timings CSV
  (superseded by the scaling regression test above).
- [x] Can we make uses accept a list instead of a dict? **Decided against,
  2026-07-02 (parked, revisit on field feedback).** Technically easy — a
  decoration-time shim normalising `['THRESHOLD', normalise]` (strings name
  module globals; functions/classes key on `__name__`) to the dict. A list
  of *actual variables* (`uses=[THRESHOLD]`) is not possible: the name is
  gone by call time, equality reverse-lookup is ambiguous and identity
  fails on interned small ints; the AST-of-the-decorator-call trick works
  but only for literal names and was rejected as spelling-dependent magic.
  Decision: the saving is one repetition per entry, while the cost is a
  second spelling of a core concept in every doc/example plus new failure
  modes (typo'd strings, `__name__` keying) — and the dict is needed anyway
  for renaming/pinning and closures, so the list can never replace it. The
  duplication's real hazard (silent shadowing) is now caught by
  `check_shadowing`. Add later if migration feedback shows real errors,
  not just aesthetics — adding is cheap, removing needs a deprecation.
