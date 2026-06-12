# SLURM executor — implementation plan

Implementation plan for the SLURM executor item, to be developed and
validated on JASMIN. The behaviour is fully specified in
[remake3_design.md](remake3_design.md) (SLURM executor and dynamic
matrices sections) and scoped in
[detailed_code_implementation.md](detailed_code_implementation.md) §4 —
this doc covers how the work proceeds, what is testable locally, and the
cluster-specific risks.

Settled questions, not to be reopened:

- Continuation-job replanning cost is measured and acceptable (~14 s for a
  fully populated 1e6-task DB; see `tests/benchmarks/`). No per-rule
  incremental planning machinery.
- Task lookup in array jobs uses direct construction
  (`Remake.task_from_spec(rule_name, kwargs)`, 0.04 ms at 1e6 tasks), not
  key search. The per-rule JSON specs carry `rule` + `kwargs` precisely
  for this.

## Implementation pieces

1. **Rewrite `executors/slurm_executor.py`** (the stale remake2 file is
   deleted as this lands). Mostly deterministic file generation:
   - `.remake/jobs/<rule>.json` — array of `{task_key, rule, kwargs}`;
     SLURM array index = position in the array.
   - `.remake/jobs/<rule>.jobids.json` — sidecar written at submission.
   - `.remake/slurm/<rule>.sbatch` — per-rule script, array-parameterised
     where eligible; SLURM options merged from Remake-level config and
     per-rule `config={'slurm': {...}}`.
   - `.remake/submit.sh` — master script: `sbatch --parsable` capture,
     `--dependency=aftercorr:`/`afterok:` wiring, jobid sidecar writes.
   - Array eligibility: no intra-rule deps ∧ same matrix as upstream ∧
     size ≥ threshold (default 10). Fan-in rules fall back to individual
     jobs with `afterok`.
   - Continuation job: rules deferred at generation time are excluded
     from `submit.sh`; a `continuation.sbatch` re-invoking
     `remake run --executor slurm` is appended after the last submitted
     wave. Idempotent replanning handles arbitrarily deep chains.
   - Already-queued detection: read jobid sidecars, check `squeue` by job
     ID + array index (never by job name).

2. **CLI additions**:
   - `resubmit <remakefile>` — re-execute `.remake/submit.sh`, no
     replanning.
   - `run --executor slurm --dry-run` — generate `.remake/jobs/`,
     `.remake/slurm/` and `submit.sh`, submit nothing.
   - `run-task` payload in sbatch scripts: pass rule + array index; the
     task spec is read from the rule's JSON and constructed via
     `task_from_spec` (extend `run-task` or add a `run-array-task`
     internal command — decide during implementation).

## Local-first testing (no cluster)

Nearly all of the executor is testable on a laptop; cluster time should
be spent only on cluster-shaped problems.

- Golden-file tests: generate specs/sbatch/submit.sh for known pipelines
  (ex2/ex4 shapes) and assert content — array eligibility, dependency
  wiring, config merging, continuation emission for ex8-style dynamic
  rules.
- Fake `sbatch`/`squeue` shims on PATH: test submission flow, `--parsable`
  jobid capture, sidecar writing, already-queued detection, `resubmit`.

## Pre-flight items (gate the cluster work, in order)

1. **Packaging** — `pip install -e .` has never been run; the dynamic
   version in pyproject may import the package at build time. A working
   install in a JASMIN env is needed on day one. (todos.md, Packaging.)
2. **Tracebacks** — `run_task` stores `repr(e)` only; debugging failed
   cluster jobs without tracebacks is miserable. Store full tracebacks
   and add `info --show-failures` first. (todos.md, Failure UX.)
3. **File logging** — restore CLI file logging (per-job logs are how
   cluster failures get debugged). (todos.md, Smaller debts.)

## Cluster-shaped risks

- **SQLite on shared filesystems — the big one.** Every array element
  does an EXCLUSIVE-locked write to `.remake/remake.db`. SQLite locking
  over Lustre/NFS-class filesystems is notoriously unreliable (locks not
  honoured, or pathological serialisation under hundreds of concurrent
  writers). Probed on JASMIN 2026-06-12
  (`tests/benchmarks/bench_sqlite_contention.py`, see todos.md): 176-way
  concurrency is clean, but 400-way and 800-way livelock — most processes
  never acquire the lock within a job's wall-time, and throughput at
  800-way is *lower* than at 400-way. Option 3 below ("per-job sidecar
  result files") is needed; see the design sketch in the next section.
  1. it works — ship it; (ruled out: 176 ok, 400+ livelocks)
  2. tune (longer backoff, busy_timeout, journal mode) — worth doing
     regardless (the current backoff is unbounded), but doesn't fix the
     400+ cliff on its own;
  3. per-job sidecar result files merged into the DB by the next
     plan/continuation run (no concurrent DB writers at all) — **chosen**,
     see below.
- **Scheduler quirks**: `aftercorr` semantics with partially-failed
  upstream arrays; JASMIN array-size and queued-job limits; partition
  names and accounting. Defaults updated 2026-06-12 after checking this
  JASMIN node: `short-serial`/`long-serial` no longer exist (SLURM 25.11);
  `DEFAULT_SLURM_CONFIG` now uses `partition=standard, qos=standard`.
  `--account` is project-specific and not defaulted — set via
  `Remake(config={'slurm': {'account': ...}})`. Array throttling
  (`--array=0-N%T`) is supported via `config['slurm']['array_throttle']`.

## Sidecar result files — design sketch (2026-06-12)

> **Implemented 2026-06-12** (`metadata/sidecar.py`,
> `Sqlite3Backend.ingest_sidecars`, ingest called from `Remake.plan()`).
> Decision on the open question below: only `run-array-task` writes
> sidecars (it loads without finalizing — no `ensure_rules`, no DB
> connection at all); `run-task` keeps direct `update_task`.

Replaces direct `update_task` calls from per-task SLURM processes with a
write-sidecar / ingest-serially split, mirroring the per-task-logging
layout (`design_docs/per_task_logging.md`).

### Write side: `run-task` / `run-array-task`

These per-task-process entry points currently end by calling
`self.metadata.update_task(task, status, exception)` (in
`Remake.run_task`, `core/remake.py`). Instead, give `Remake` a metadata
backend that writes a **sidecar file** rather than touching
`.remake/remake.db` at all:

```
.remake/tasks/results/<rule>/<key[:2]>/<key[2:]>.json
{"status": 0, "exception": "", "uses_hash": "...", "timestamp": "..."}
```

- Same sharding scheme as the per-task logs — reuses the directory-count
  reasoning already written up.
- `run-task`/`run-array-task` never call `ensure_rules`/open
  `remake.db` — they don't need `rule_ids`, so no DB connection at all
  from the hundreds of concurrent array processes. This removes the
  contention source entirely, not just the write.
- `run` (singleproc executor, in-process serial loop) keeps calling
  `update_task` directly — single writer, no problem, and it's the
  fastest path for small/local pipelines.

### Read side: ingest at the start of `plan`/`run`/`info`

Add `Sqlite3Backend.ingest_sidecars(rules)`:

- Walk `.remake/tasks/results/<rule>/**/*.json` for each rule.
- One `BEGIN EXCLUSIVE` transaction for the *whole batch* (this is the
  single-writer case — batching is safe and turns O(N) locked
  transactions into O(1), which also helps the existing "one EXCLUSIVE
  transaction per task" scaling todo for this path).
- For each sidecar: `update_task(...)` as today, then delete the sidecar
  file. If the file is gone by the time it's read (another process
  ingested it first), skip — `update_task`'s `ON CONFLICT DO UPDATE` makes
  double-ingestion harmless anyway, so this is belt-and-braces.
- Call this once, early, from `Remake.plan()` (and therefore `run()`) and
  from `info`/`--show-failures`. Cheap when there's nothing to ingest
  (one directory walk); proportional to *new* completions since the last
  invocation otherwise.

### Why this answers "is the DB still the source of truth?"

Yes, with one caveat: a sidecar represents "completed but not yet
ingested." Any invocation that *reads* the DB for planning or reporting
ingests first, so by the time `plan()`/`info` actually looks at the DB
it's current. The only window where the DB is stale is between a task
finishing and the *next* `remake` invocation — and nothing currently reads
the DB during that window anyway (the running array job doesn't need to;
`event_matrix()`-style dynamic matrices read output files directly, not
the DB).

### Interaction with already-queued detection

Independent and complementary. The squeue + `.remake/jobs/<rule>.jobids.json`
check (already implemented) decides whether to *resubmit* a rule's array;
sidecar ingestion decides what the DB *says* about completed tasks. A
rerun while rule X's array is still in flight: ingestion picks up whatever
sidecars exist so far (possibly none), the squeue check skips resubmitting
X regardless of what the DB now shows for X's tasks.

### Open questions / follow-ups

- Sidecars are deleted on ingest, so they don't accumulate like the
  per-task logs do — no separate retention story needed.
- `Remake.__init__`/`plan()` currently calls `ensure_rules` unconditionally;
  `ingest_sidecars` needs the same `rule_ids` map, so it likely runs
  immediately after `ensure_rules`, same place.
- ~~Not yet decided: does `run-task` (non-array, used by singleproc/multiproc
  executors) also switch to sidecars, or only `run-array-task`?~~ Decided
  and implemented: only `run-array-task` writes sidecars; `run-task` keeps
  calling `update_task` directly (single writer).
- Remaining validation: rerun the contention stress at 400/800-way on
  JASMIN through the real pipeline path to confirm the livelock is gone
  (bench_sqlite_contention.py exercises the old direct-write path by
  design — it remains useful for measuring the ingest-side transaction).

## Suggested order

1. ~~Pre-flight: packaging verification, tracebacks, file logging.~~ Done
   2026-06-12.
2. ~~Executor generation (stage 1+2) + golden-file tests, locally.~~ Done:
   `executors/slurm_executor.py` rewritten; tests in
   `tests/integration/test_slurm.py`. The run-task payload decision:
   a `run-array-task <remakefile> <rule> <index>` subcommand reading the
   rule's JSON spec and using `task_from_spec`. Already-queued detection is
   per rule, not per task — rewriting a rule's JSON spec while its previous
   array is queued would corrupt the indices those jobs read, so a rule with
   any queued elements is skipped wholesale and picked up by a later run.
3. ~~Submission flow + fake sbatch/squeue shim tests, locally.~~ Done.
4. JASMIN: install, run ex2/ex4 for real; probe SQLite contention with a
   wide array job; then ex8-style continuation chains.
5. Delete the stale remake2 multiproc executor when ported (slurm done);
   tick the implementation-plan item after JASMIN validation.
