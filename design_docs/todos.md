# Todos

Concrete known problems and debts, ordered roughly by severity. Completed
entries are pruned to [todos_archive.md](todos_archive.md) at each release
(last prune: 2026-07-03, at 0.8.0).

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
- [ ] Make the scale benchmarks load-bearing in CI. **Reframed 2026-07-02:**
  the one that should gate is `bench_field_scale.py` (the design shape,
  ~5 s total — cheap enough to run as a test with generous thresholds);
  `bench_million_tasks.py` stays as the manual stress-headroom check.
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

## Smaller debts

- [ ] `eval`-based query filter (see MM comment in `core/planner.py:27`):
  `make_predicate` does `eval(compile(query, ...))` against task kwargs.
  Hardened (`__builtins__` stripped, kwargs as the only locals) and the query
  is the user's own, so the threat model is low — but it is not a sandbox
  (dunder traversal on passed objects, resource exhaustion). Replace with a
  restricted-ops parser that walks the AST and allow-lists a small set of
  comparisons/boolean ops (the pyquerylist approach), which also yields better
  error messages than a bare `NameError`/`SyntaxError`. Pre-1.0 fix; revisit
  at CLI work.
- [ ] **`planner.plan` is long and interleaves three concerns** (MM comments,
  `core/planner.py:278-279`): matrix-deferral, per-task freshness, and rerun
  propagation (the same-pass `rerun_kwargs` path *and* the durable `run_seq`
  backstop). Extract the per-task decision (the body of `for task in tasks`)
  into a `_task_rerun(task, rec, ...) -> (rerun, reason)` helper so `plan` reads
  as orchestration. Behaviour-preserving readability refactor; well covered by
  the planner tests.
- [ ] No Hypothesis property tests despite the design doc promising them
  (task key uniqueness/stability, matrix normalisation).
- [ ] **Bound and message-match `retry_lock_commit`**
  (`sqlite3_backend.py`): it catches *any* `OperationalError` (not just
  "database is locked" — also "no such table", disk-full, malformed schema)
  and retries forever with growing backoff, so a genuine error becomes a
  silent hang. Match on the lock message, re-raise others, cap attempts.
  The sidecar design removed the high-concurrency pressure (livelock
  history in [todos_archive.md](todos_archive.md)), so this is robustness,
  not the old livelock. Also flagged in the 0.8.0 release plan §E and
  discussion.md ("the wrong primitive").
- [ ] `ZarrStore.is_complete()` checks `.zmetadata` (zarr v2); zarr v3
  consolidated metadata lives in `zarr.json`. Handle both when xarray/zarr
  versions move.
- [ ] **Per-task log total-file-count budget** — the sharded
  `.remake/tasks/log/` tree grows one file per task with no cap; see the
  open question in design_docs/per_task_logging.md.

## SLURM

- [x] **2026-07-09 submission-logic review findings — CLOSED 2026-07-10**
  (full report: [code_reviews/2026-07-09_review.md](code_reviews/2026-07-09_review.md)).
  Landed, in order: per-submission immutable spec files (root fix — findings
  1–4/10a/12 downgraded or fixed); squeue-failure fail-safe (`SqueueError`;
  run/resubmit refuse over an unknown queue, slurm-status errors cleanly);
  squeue timeout; finding 3 (active = any non-terminal state); finding 5's
  live half (run-array-task asserts rebuilt key == `spec['task_key']`; matrix
  case already blocked at plan time, e3d5183); finding 11 (paths
  shlex-quoted); finding 6 (`check_resubmit_safe`: refuses on queued jobs,
  squeue failure, or stale literal dependency ids); findings 8/9
  (no dependency-less continuation; `--kill-on-invalid-dep=yes` on the
  continuation); age-based spec pruning (>7 days, sidecar-referenced kept);
  finding 7 (aftercorr only when element-wise correspondence is *proved*
  from task inputs/outputs, else afterok); C2 (arrays-everywhere — individual
  mode removed, which also removes C1's dual sidecar encoding; legacy
  `slurm_job_ids` sidecars still read).
  **Parked** (Mark; design + revival notes in
  [slurm_already_running.md](slurm_already_running.md)): per-task skip,
  `--comment` job stamping, and the per-rule submission ledger both it and
  provable pruning would need — too complicated for minimal payback.
  **Archived without action** (rationale): 10b (dry run stages submit.sh) —
  the dangerous half (resubmit executing a dry plan blind) is covered by the
  resubmit guard, and "generate everything, submit nothing" is exactly what
  makes dry run useful for inspection; C3 (each array element parses the
  whole spec file) — low MBs of JSON at target scale, negligible; C4/C5
  (minor duplication) — fold in opportunistically when that code is next
  touched.
- [x] ~~Check behaviour of deferrable rules under SLURM~~ — **resolved
  2026-07-10** (the note said "downstream" but meant *upstream*). Verified
  empirically end-to-end: editing the upstream of a complete dynamic
  pipeline (a) under singleproc reruns the deferrable rule's tasks in the
  same invocation (wave replan loop); (b) under SLURM defers the deferrable
  rule ("matrix would expand from stale upstream output") and submits a
  continuation pinned afterok on the upstream, which replans and resubmits
  it — regression test
  `test_upstream_rerun_defers_deferrable_rule_to_continuation`; (c) durable
  `upstream-newer` propagation covers a lost continuation at the next
  invocation. The bug as observed on JASMIN (~2026-06-13/15) was real but
  predated 1fc16c4 (2026-06-17, defer @deferrable matrices on stale
  upstream); a failed upstream element could also silently park the
  continuation until finding 9's `--kill-on-invalid-dep` fix (2026-07-10).
  Note: `remake why` does not surface "deferred because upstream reruns" —
  it reports only the upstream's own reasons (minor UX gap, `info` does show
  deferred rows).

## UX

- [ ] Run-start observability (lesser remainder of the 2026-07-02
  observability work, main asks archived): log chosen executor/nproc/config
  at run start; the direct-DB-write-vs-sidecar decision and per-task
  `update_task` are unlogged (trace would suffice).
