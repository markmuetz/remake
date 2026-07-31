# Per-task resource capture

> **Status: design agreed (Claude + MM, 2026-07-31); not yet implemented.**
> Target release: **0.9.0, item 1** — first in the scoped slice, because the
> run report (item 4) is only as good as the history accumulated before it
> ships ([future_releases/v0.9.0.md](future_releases/v0.9.0.md)). Class:
> **Design** — decisions below are settled; the four questions left open in
> the first pass were resolved as proposed (see Settled, at the end).

## Motivation

Snakemake's `benchmark:` records wall time and peak RSS per job. remake
today records a single `last_run_timestamp` per task: how long anything
took survives only in the JSONL log's `task_complete`/`task_failed` events
(`seconds=`), which are per-run, unqueryable and eventually rotated away.

Three concrete asks this unblocks:

1. **The run report (0.9.0 item 4)** — a table of per-rule and per-task
   durations is the report's spine. Nothing to render without this.
2. **SLURM right-sizing** — "you asked for `mem=4G`, the rule peaked at
   `620M`" is the single most useful thing remake can tell a JASMIN user.
   The `sacct` audit (0.10.x) does this post-mortem and SLURM-only; this
   does it for *all* executors, from remake's own measurements.
3. **"remake saved you N CPU-hours"** — the value metric in
   [discussion.md](discussion.md) needs historical durations to multiply
   stale-skips by. 0.9.0 gets the per-task numbers; the rollup is 0.10.x.

## Decisions

### 1. Storage: columns on `task` in `remake.db` — *not* `stats.db`

[discussion.md](discussion.md) argues at length for a separate append-only
`.remake/stats.db` with one row *per execution* (full history, per-run
grain, derived aggregates). That remains right — and it stays **0.10.x**
(the roadmap's `remake stats` item). 0.9.0 does the smaller thing:

**Four nullable columns on the existing `task` table, holding the *last*
execution's numbers**, upserted by the write that already happens.

```sql
ALTER TABLE task ADD COLUMN wall_s REAL;          -- seconds, wall clock
ALTER TABLE task ADD COLUMN cpu_s REAL;           -- seconds, user+sys
ALTER TABLE task ADD COLUMN max_rss_bytes INTEGER;
ALTER TABLE task ADD COLUMN rss_method TEXT;      -- 'sample' | 'rusage' | NULL
```

Why last-only in the operational DB rather than history in a new one:

- **Zero new writes.** The values ride the existing `_upsert_task` /
  `_ingest_records` statements. No second transaction per task — the thing
  [todos.md](todos.md) already flags as the per-task write cost.
- **Nothing to read them but us.** The planner never touches these columns,
  so they cannot become a rerun trigger; a pre-upgrade record with NULLs is
  simply "not measured".
- **A run touches each task once**, so last-only *is* complete data for the
  run report of the run you just did — which is 0.9.0's consumer.
- **The measurement is the reusable part.** `stats.db`, when it lands,
  records from the same hook and the same numbers; only the sink is new.
  Capturing now is what gives 0.10.x a populated `wall_s` to migrate
  history from, and RO-Crate its `startTime`/wall-time fields
  ([rocrate_export.md](rocrate_export.md) §75).

Cost: ~28 bytes/task — 280 KB at the 1e4-task design scale, ~28 MB at the
1e6-task stress figure. Acceptable; contrast the 272 MB inline-uses
disaster this DB was reworked to escape.

**`rss_method` earns its column** because the three executors measure by
different means with different accuracy (§3): without it the report would
silently compare a sampled number against an interpreter-inclusive
`getrusage` one. NULL = not measured.

### 2. Output bytes belong to `output_stat`, not here

The obvious fourth resource — bytes written — is already designed, as the
`output_stat(task_id, name, n_files, total_bytes)` table in
[dir_outputs.md](dir_outputs.md) §5, populated per *output name* by the
completion scan. Resource capture does not duplicate it; the report joins
the two. This also fixes the ordering: both are schema additions and both
should land in the **same early `_add_missing_columns` pass** as the
checksum column (0.9.0 item 2), so there is one migration and one
upgrade-no-mass-rerun test, not three.

### 3. Measurement: one hook, three accuracies

`Remake.run_task` (`core/remake.py:727`) is the single execution chokepoint
— every executor goes through it, and it already brackets the call with
`perf_counter()`. All capture happens there, in the process that runs the
task, and both exit paths record (see §4).

- **`wall_s`** — the `perf_counter()` delta already computed. Free.
- **`cpu_s`** — `resource.getrusage(RUSAGE_SELF)` + `RUSAGE_CHILDREN`
  utime+stime, *deltas* across the task. Free, and it is what exposes a
  4-core allocation running single-threaded work. Deltas are sound for CPU
  time (it accumulates); they are **not** for RSS, which is the next point.
- **`max_rss_bytes`** — the hard one, because `ru_maxrss` is a *process
  high-water mark that never decreases*. In singleproc the task runs in the
  parent, and in multiproc/dask a pooled worker runs many tasks in
  sequence: reading `ru_maxrss` at task end would report the peak of every
  task that worker ever ran, silently attributed to this one. A delta is no
  better (0 or meaningless). So:

  - **Primary — `'sample'`:** a daemon sampler thread reads
    `/proc/self/statm` every `rss_interval` (default 100 ms) and keeps the
    max; one sample is forced at start and at end so a sub-interval task
    still gets a value. Linux-only, stdlib-only (no psutil — dependency
    minimalism), ~30 lines. Where a task shells out (`cdo`, `ncks` — normal
    in this user base), `RUSAGE_CHILDREN.ru_maxrss` is *also* read at start
    and end and the result is `max(sampled_self, children_end)` when the
    children high-water grew during the task.
  - **Fallback — `'rusage'`:** where `/proc` is unavailable (macOS, BSD)
    **and** the process runs exactly one task — `remake run-array-task`
    (SLURM) and `remake run-task` — `RUSAGE_SELF.ru_maxrss` at the end is
    correct to within the interpreter baseline. Recorded as `'rusage'`.
  - **NULL otherwise.** No `/proc` *and* a task-reusing process:
    record nothing rather than a number that is wrong by construction.
    This is the honest failure mode, and `rss_method` makes it legible.

  **Known inaccuracies, to be documented, not hidden:** sampling misses a
  spike shorter than the interval (a task that allocates 40 GB for 20 ms
  under-reports); `statm` RSS counts shared pages, so shared libraries and
  page-cache-backed mmaps inflate it; `ru_maxrss` is **KiB on Linux, bytes
  on macOS** (the classic trap — normalise at the source, store bytes
  always). Under SLURM the cgroup's `memory.peak` would be the accurate
  answer; deferred to a later `rss_method` value pending a field
  measurement (Settled §1), and the `sacct` audit remains the authoritative
  post-mortem.

Sampler cost at scale: one thread and one small `/proc` read per 100 ms
*per concurrently running task* — bounded by `nproc`, not by task count.
Negligible against the tasks remake exists to run; still disable-able (§6).

### 4. Failures are captured too

A task that fails after three hours is the most valuable duration in the
DB. The `except` branch of `run_task` already computes `elapsed`; it
records resources alongside `TASK_STATUS_FAILED`.

**Blind spot, stated plainly:** a task the kernel or SLURM *kills* (OOM,
timeout) never reaches either branch, writes no sidecar, and leaves no
record at all — which is exactly the case a memory number would be most
wanted for. remake cannot fix this from inside the dying process; it is
what the 0.10.x `sacct` audit is for. Do not let the report imply
otherwise (a killed task shows as `pending`/no-record, as today).

### 5. Crossing the process boundary

`MetadataManager.update_task(task, status, exception='')` gains an optional
`resources=None` keyword — a small dict (`{'wall_s':…, 'cpu_s':…,
'max_rss_bytes':…, 'rss_method':…}`), passed as a keyword, ignored by
backends that don't want it.

- **`Sqlite3Backend._upsert_task`** — three more bind params in the
  existing INSERT…ON CONFLICT. `None` → NULL.
- **`SidecarWriter.update_task`** — payload gains a `resources` key
  (`metadata/sidecar.py`). This is the SLURM/multiproc/dask path: the
  measurement happens on the compute node, the write happens at ingest.
- **`_ingest_records`** — reads `payload.get('resources') or {}`; a
  pre-0.9 sidecar in flight during an upgrade simply lands NULLs. Same
  forward-compat shape as `run_hash`/`io_hash` before it.
- **`TaskRecord`** gains the four fields, defaulting to `None` (pre-upgrade
  records), and `get_tasks_status` selects them.

This is a public-API change to an ABC in a minor release — permitted
pre-1.0 ([compatibility.md](compatibility.md): "break freely, record in
CHANGELOG"), and additive-with-default, so the only breakage is a
third-party backend that overrode `update_task` with a strict signature.
CHANGELOG entry under **Changed**, one-line migration.

### 6. Config

```python
Remake(config={'resources': {'capture': True, 'rss_interval': 0.1}})
```

**Default on.** `wall_s`/`cpu_s` are free and always recorded; `capture:
False` turns off only the RSS sampler thread. Deliberately unlike the
checksum knob (0.9.0 item 2), which defaults *off* because it re-reads
every byte — sampling costs a thread and a `/proc` read, and observability
nobody enables is worthless. The knob exists for the pathological case
(microsecond tasks, a profiler that dislikes the thread).

### 7. Surfacing in 0.9.0 — deliberately thin

- **`task-info`** — three lines (`wall`, `cpu`, `peak rss`, the last
  annotated with `rss_method` when it is not `'sample'`), plus the fields
  in `--json`. `Remake.task_info` returns them; the CLI renders.
- **JSONL events** — `task_complete`/`task_failed` bind the same fields
  they now measure. Free, and it keeps log-based analysis working.
- **The run report (item 4)** is the real consumer, and can already group
  by `run_seq` (stamped per invocation on every task row), so 0.9.0 needs
  **no `run` table**.
- **`info` is left alone.** A per-rule mean/p95 column is tempting and
  cheap SQL, but aggregates are the `remake stats` design (0.10.x); adding
  a half-version here would have to be undone. Scope discipline.

## Implementation order

1. `util/resources.py`: `capture_resources(interval)` context manager —
   wall/cpu deltas, the sampler thread, `/proc` detection, `ru_maxrss` unit
   normalisation, `rss_method` selection.
2. Wire into `run_task` (both exit paths) + the config knob.
3. `update_task(..., resources=None)` through the ABC, `Sqlite3Backend`,
   `SidecarWriter`; `TaskRecord` + `get_tasks_status`.
4. Schema columns in `_add_missing_columns` — **one migration pass shared
   with `output_stat`/`checksum`** (0.9.0 items 2 and 6).
5. `_ingest_records` reads `resources` from the sidecar payload.
6. `task_info` + CLI rendering + JSONL event fields.
7. Docs page (what is measured, the three accuracies, the OOM blind spot,
   the shared-page/interval caveats) and CHANGELOG.

## Tests

- **Known allocation:** a task that holds ~200 MB → peak ≥ 200 MB and
  within a generous upper bound. Generous thresholds only — this is a
  measurement, and the CI machine is not the field (the empirical-rigour
  rule: one clean run proves nothing about a shared node).
- **Pooled-worker attribution:** a big task followed by a tiny one in the
  *same* multiproc worker → the tiny one must not inherit the big peak.
  This is the bug the sampler exists to prevent; test it explicitly.
- **Subprocess child:** a task that shells out to an allocating child.
- **Duration:** a sleeping task → `wall_s` within tolerance; a spinning
  task → `cpu_s` ≈ `wall_s`.
- **Failure path** records resources with `TASK_STATUS_FAILED`.
- **Sidecar round-trip:** SLURM/multiproc path lands the same numbers via
  ingest; a payload *without* `resources` ingests cleanly as NULL.
- **Upgrade:** an existing 0.8.x DB migrates and causes **no mass rerun**
  (the promise 0.8.1 made; shared with the other 0.9.0 schema items).
- **No `/proc`:** monkeypatched-absent → NULL + `rss_method` NULL, no
  crash, task still runs.

## Settled (MM, 2026-07-31 — all four proposals accepted)

1. **No cgroup `memory.peak` in 0.9.0.** It would be more accurate than
   `statm` (no shared-page inflation, catches sub-interval spikes) and is
   readable from inside a job on cgroup v2 — but path/permission
   variability across sites is exactly the kind of thing that works on one
   cluster and not the next. Revisit **with a real JASMIN measurement in
   hand**, as a fourth `rss_method` value (`'cgroup'`); the enum is
   designed to take it without a schema change.
2. **`rss_interval` defaults to 100 ms.** 10 ms would catch shorter spikes
   at 10× the reads; not paid for speculatively. Revisit against field
   data — the knob is already user-settable, so a site that needs finer
   sampling has it without a release.
3. **No host/node name.** Cheap and useful for "why is this rule slow on
   node X", but it is per-execution `stats.db` data (0.10.x) and mildly
   identifying (the privacy note in [discussion.md](discussion.md)).
4. **`set-state` does not clear the columns.** Marking a task pending by
   hand does not un-measure what ran; the columns describe the last actual
   execution, and `rss_method` NULL already means "unmeasured". Consumers
   (the report) must therefore read them as *last execution*, not as
   "current state" — worth a line in the docs page (step 7).

## Release

0.9.0, item 1, landing before items 2/4/6 (the report consumes it; the
schema addition rides with theirs). Minor-release gates apply:
`/code-review ultra` pre-tag, upgrade-no-mass-rerun test, CHANGELOG entries
for the `update_task` signature and the new `task-info` fields.
