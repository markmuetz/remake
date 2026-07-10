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

- [ ] **Fix the 2026-07-09 submission-logic review findings** — 12 correctness
  issues (11 confirmed) + 5 cleanups:
  [code_reviews/2026-07-09_review.md](code_reviews/2026-07-09_review.md).
  Root theme: the "never rewrite a spec a queued array still reads" invariant
  has a single guard (already-queued detection) that fails open three ways
  (squeue error swallowed at debug, mid-rule sbatch failure under `set -e`
  before the sidecar echo, suspended/held states not counted as active).
  **Per-submission immutable spec files (`<rule>.<run_seq>.json`, referenced by
  the sbatch script) fix findings 1–4 and 12 at the root** — spec rewrites can
  never corrupt queued arrays, and the executor can then submit exactly the
  not-yet-queued tasks instead of skipping whole rules. **Landed 2026-07-10**
  (specs immutable per run_seq, sbatch pins via `--specs`, sidecar records
  run_seq, task-info index pinned): findings 1–3 and 12 are downgraded from
  index corruption to duplicate submission, finding 10a fixed. Still open from
  that cluster: ~~squeue-failure fail-safe~~ (landed 2026-07-10:
  `squeue_snapshot` raises `SqueueError`, run refuses to submit over an
  unknown queue when submissions are recorded, slurm-status errors cleanly),
  per-task skip (finding 4), and pruning
  accumulated `<rule>.<run_seq>.json` files (one per rule per submission/
  dry-run; a prune is only safe when the sidecar's recorded submission has
  left the queue). Small fixes batch landed 2026-07-10: squeue timeout →
  SqueueError, finding 3 (active = any non-terminal state, inverted filter),
  finding 5's live half (run-array-task asserts rebuilt key == submitted
  `spec['task_key']`; the matrix-sourced case was already blocked at plan
  time by dag scalar check, e3d5183), finding 11 (remakefile/specs paths
  shlex-quoted in all three templates). Still open, independent of that:
  resubmit with no queue check + baked literal dependency ids, aftercorr chosen
  by kwargs equality not data dependence, continuation self-replication +
  missing `--kill-on-invalid-dep`, dry-run staging submit.sh (10b). Overlaps
  the per-task already-running item below (findings
  1 and 4 are its motivating bugs; design in
  [slurm_already_running.md](slurm_already_running.md)) — land them together.
- [ ] Per-task "already running?" detection. Rule-level skipping exists
  (`squeue_snapshot`/`_active_jobids`/`_queued_jobids` skip a whole rule whose
  last submission is still PD/R); make it per-task and replan-proof by stamping
  each job with a run id + its spec path so a queue snapshot maps back to the
  exact remake task. (The latent resubmit-all bug — a *failed* squeue read as
  an empty queue — was fixed separately 2026-07-10, `SqueueError` fail-safe.)
  Design: [slurm_already_running.md](slurm_already_running.md).
  (Once landed, `run -E slurm`'s per-rule submission line is the natural home
  for a "skipped N already-queued tasks" message.)
- [ ] Check behaviour of deferrable rules under SLURM. When running, I think
  that the downstream tasks rerunning should have triggered a rerun of the
  deferrable jobs but did not. Worth checking.

## UX

- [ ] Run-start observability (lesser remainder of the 2026-07-02
  observability work, main asks archived): log chosen executor/nproc/config
  at run start; the direct-DB-write-vs-sidecar decision and per-task
  `update_task` are unlogged (trace would suffice).
