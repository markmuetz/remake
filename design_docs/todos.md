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
- [ ] **`remake info` queries every rule's status twice**
  ([bug 04](bugs/04_info_redundant_and_superlinear_status_queries.md)
  Issue 1). The planner fetches per-rule task status to compute the plan,
  then the info renderer re-runs the identical `get_tasks_status` queries to
  build the table — pure duplication within one read-only invocation
  (~2.7 s of the measured ~10 s on a 3k-task pipeline). Thread the
  planner's already-fetched status through to the renderer (or render from
  the plan result). Independent of — and worth doing regardless of — the
  storage rework above; on an all-settled pipeline every rule pays the
  double query.
- [ ] **Status-query regression micro-benchmark** (logs_analysis §4.5). Replay
  a large DAG's plan against a populated DB and assert status-query time stays
  ~O(distinct code), not O(tasks × source size) — the field logs only revealed
  the 256× amplification after the fact. Natural home: alongside
  `tests/benchmarks/bench_million_tasks.py` and the CI-benchmark todo above.

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
- [ ] rule plumbing errors should be treated differently from individual task 
  errors. If e.g. an inputs fn doesn't have the correct args for the var matrix,
  this should be raised immediately with a helpful error message, as it will 
  apply to all tasks.
- [ ] Just as there is a `remake task-info`, there should be a `remake rule-info`.
  It should display the rule's docstring at the top. And it should give useful info
  about the rule, including its matrix, input and output *templates*. These can
  be defined even if they are set by functions: just pass in e.g. case='{case}'.
  

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
- [ ] **`planner.plan` records only the first rerun trigger** (MM comment,
  `core/planner.py:354`). `reason` is overwritten, so code-changed shadows
  uses-changed shadows io-changed when several are true at once. The dry-run
  "why" view therefore shows one cause when there may be several. To give full
  fidelity, collect a list of reasons instead of overwriting (then surface them
  in `why`/`info --reasons`). Minor, but a real fidelity gap.
- [ ] **`planner.plan` does redundant work when `force` is set** (MM comment,
  `core/planner.py:369`). The per-task loop computes `rec`, runs the code
  compare, the `uses`/`io` hash compares and `_outputs_complete`, then
  unconditionally overwrites the verdict with `'forced'`. Hoist `if force:` to
  the top of the task loop and skip straight to appending — saves the
  hash/compare/stat calls per task, which matter at 1e6 tasks. Behaviour-
  preserving; covered by existing force tests.
- [ ] **`planner.plan` is long and interleaves three concerns** (MM comments,
  `core/planner.py:278-279`): matrix-deferral, per-task freshness, and rerun
  propagation (the same-pass `rerun_kwargs` path *and* the durable `run_seq`
  backstop). Extract the per-task decision (the body of `for task in tasks`)
  into a `_task_rerun(task, rec, ...) -> (rerun, reason)` helper so `plan` reads
  as orchestration. Behaviour-preserving readability refactor; well covered by
  the planner tests.
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
- [ ] Execution observability: per-task timing + a run summary. Logging is
  well-structured (debug-summarises / trace-per-element, followed consistently)
  and planning/metadata are well covered with counts and timings, but there is
  **no timing in execution** — `perf_counter` appears only in the planner and
  metadata, nowhere in the executors or `run_task` (verified by grep). For a
  tool built for long SLURM pipelines this is the biggest logging gap. Concrete
  asks, in priority order:
  1. **Per-task duration.** Executors log a task's *start* (`3/100: process[n=3]`)
     but never its completion or elapsed time. Add timing in the `Executor`
     base / `run_task` so all four executors (singleproc/multiproc/dask/slurm)
     get it uniformly — completion + duration at `debug` (per the "summarise
     loops" convention; avoid per-task `info` spam). This is the raw material
     for the MaxRSS/wallclock resource-advice idea and the parked stats /
     run-history store (discussion.md) — a lightweight logging precursor.
  2. **Run-level summary at `info`.** `run()` returns `nfailed` and only logs on
     failure; add a one-line success summary, e.g. "ran 100 task(s), 0 failed
     in 42.3s".
  3. **`run_task` debug line with resolved I/O paths.** It currently logs only
     on failure; a `debug`/`trace` "running {task}" with the resolved
     input/output paths would show what a task actually read/wrote — the logging
     side of the `io_hash` recorded-vs-current item above.
  - Lesser: log the chosen executor/nproc/config at run start (debug); the
    direct-DB-write-vs-sidecar decision and per-task `update_task` are unlogged
    (trace would suffice).
- [ ] **Don't TRACE-log full function bodies in `code_compare`**
  (logs_analysis §3.1). The TRACE dump prints the entire source of both
  versions on every comparison — ~55k of the 56.8k lines in the worst field
  log, crowding real history out of the 5 MB rotation window. Log a one-line
  summary (`code_compare: <rule> unchanged` / `changed (N lines differ)`) and
  gate the full-body dump behind an explicit opt-in (e.g. `REMAKE_LOG_CODE=1`).
- [ ] **Split the file log into human + debug streams; threshold the timing
  lines** (logs_analysis §3.2/3.3). Keep `remake.log` readable (INFO+ run
  narrative) and route the DEBUG/TRACE firehose to a separate rotated
  `remake.debug.log` (on by default at DEBUG) so the two don't compete for
  the 5 MB window. Demote the per-query `get_tasks_status ... in Xs` line to
  TRACE or emit it only above a threshold (e.g. >100 ms) — ~1857 of them
  dominate the DEBUG stream today.
- [ ] **Structured logging for mineability** (logs_analysis §4). The field
  analysis needed fragile regex over prose lines. Add a JSONL sink
  (`logger.add(..., serialize=True)`), bind metrics as `extra` fields rather
  than interpolating into messages, tag events with a stable `event=` key,
  and stamp a per-invocation run id so one `remake run`'s lines group
  together. Lower priority than the fixes above — infrastructure, not a
  perf win.
- [ ] Can we make uses accept a list instead of a dict?
