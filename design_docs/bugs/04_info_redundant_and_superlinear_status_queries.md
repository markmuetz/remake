# Bug 04 — `remake info` re-queries task status a second time, and the per-rule status query scales superlinearly

**Status:** **all fixed** 2026-07-02. Issues 2 & 3: commit 4407d34
(`perf(metadata): task rows carry code-table FKs, not inline text`).
Issue 1: a per-invocation `RecordCache` (metadata_manager.py) shared by
the plan pass and the renderer/explainers, so each task's record is
fetched from the backend at most once per read-only command (`info`,
`why`); verified by spy tests (each key fetched exactly once) and via
`-D` (one `get_tasks_status` per rule, was two). The audit found `why -Q`
had a worse variant — the durable-propagation check re-queried each
upstream rule's full record set once per explained task (N×M) — fixed by
the same cache. `run`/`set-state` deliberately keep uncached reads
(records change under them). Reported 2026-07-02.
**Affects:** `remake info` (and any command that plans then also renders
per-rule status); the SQLite metadata backend's `get_tasks_status`.
Local and SLURM. Correctness is **not** affected — results are right,
just slow.
**Reported by:** Mark Muetzelfeldt — hit on
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py` (`-D info -F`, a
3031-task pipeline).

## What happened

`remake -D info -F wescon_radar_dev.py` took **~10.2 s** wall
(15:49:33.18 → ~15:49:43.35). The `-D` log attributes it almost
entirely to the metadata backend, in two distinct ways.

### Timing breakdown (from the `-D` log)

| Phase | Cost |
|---|---|
| Python startup + import remakefile + `ensure_rules` | ~2.7 s |
| `ingest_sidecars` (3 results) | 0.72 s |
| Status queries during **planning** (774 + 774 + 18 + 1465) | **~3.24 s** |
| Status queries during **info display** (same 774 + 774 + 18 + 1465 again) | **~2.70 s** |

The ~2.7 s startup is the remakefile's own heavy imports
(`scipy.stats` 0.95 s, `pandas` 0.70 s, `matplotlib` 0.39 s, measured
directly = 2.86 s) — a user-side concern, not a remake bug, and noted
only to account for the wall time. The remaining **~6.7 s is the DB
backend**, and it exposes two independent issues.

## Issue 1 — status is queried twice per invocation

The planner queries every rule's task status to compute the plan, then
`info` queries the **identical** set again to render the table. The `-D`
log shows each rule's `get_tasks_status` appearing twice with matching
task counts:

```
15:49:37.177  get_tasks_status ... 774 task(s) ... 0.550s   # planning
15:49:37.650  get_tasks_status ... 774 task(s) ... 0.455s   # planning
15:49:37.672  get_tasks_status ... 18  task(s) ... 0.016s   # planning
15:49:40.249  get_tasks_status ... 1465 task(s) ... 2.218s  # planning
...  plan: 2 runnable, 8 deferred in 3.751s
15:49:40.807  get_tasks_status ... 774 task(s) ... 0.466s   # info display
15:49:41.304  get_tasks_status ... 774 task(s) ... 0.495s   # info display
15:49:41.320  get_tasks_status ... 18  task(s) ... 0.015s   # info display
15:49:43.353  get_tasks_status ... 1465 task(s) ... 1.727s  # info display
```

The second block (~2.70 s) is pure duplication of the first — the DB has
not changed between them within a single read-only `info`. Threading the
planner's already-fetched status through to the renderer (or having
`info` render from the plan result instead of re-querying) removes the
whole ~2.70 s. This is the single clearest win.

Note the eight downstream rules are `deferred` here (a deferrable matrix
with an upstream rerunning), so they contribute no status query — the
duplication is only over the four non-deferred rules, and still costs
~2.7 s. On an all-settled pipeline (nothing deferred) every rule would be
queried twice, so the effect is larger in the common "nothing to do"
case.

## Issue 2 — `get_tasks_status` scales superlinearly in task count

Within a single pass, cost grows faster than task count:

| tasks | chunks | time (planning) | time (display) |
|---|---|---|---|
| 18 | 1 | 0.016 s | 0.015 s |
| 774 | 1 | 0.550 / 0.455 s | 0.466 / 0.495 s |
| 1465 | 2 | 2.218 s | 1.727 s |

774 → ~0.5 s but 1465 → ~2.2 s: roughly **2× the tasks for ~4× the
time**. So `compare_delta_z_candidates` (1465 tasks) alone accounts for
~4 s across the two passes — more than the other three rules combined.
The jump coincides with the query splitting into "2 chunks", which hints
the extra cost is in the chunking/merge or per-row Python-side handling
rather than a single indexed SQL round-trip. Whatever the cause, a
per-rule status fetch that is ~0.5 s at 774 tasks should not be ~2.2 s at
1465; the backend wants profiling at this size (query plan, index
coverage on the task-key lookup, chunk size vs SQLite's variable limit,
and any per-row object construction).

**Root cause — the `code.code` JOIN in `get_tasks_status`.** Confirmed
against source by the field-log analysis in
[logs_analysis/README.md](../logs_analysis/README.md) §1.1–1.2. The query
is `SELECT ... FROM task LEFT JOIN code ON task.run_code_id = code.id`
(`sqlite3_backend.py:248`), which pulls back the **full stored run source
(`code.code`) for every task**, so the planner can feed it into
`CodeComparer()(rec.run_code, run_src)` (`planner.py:201`/`:358`) to
decide whether each task's code changed. The `code` table is
content-addressed and tiny (wescon-tools: 74 distinct rows, 149 KB), but
the JOIN re-materialises a full ~2 KB source copy per task — **38.4 MB
fetched for 3341 tasks, a 256× amplification**. That is the mechanism
behind the superlinear scaling: cost tracks the number of previously-run
tasks (those with stored code to fetch and compare), and bigger DAGs
touch bigger rules, so ms/task roughly doubles 774 → 1465. (My earlier
draft of this paragraph guessed `uses_hash` was the culprit — wrong
column: `get_tasks_status` does not select `uses_hash`. That column is a
separate problem, driving DB *size* not query *time* — see Issue 3
below.)

**Fix (implemented 2026-07-02, commit 4407d34).** `get_tasks_status` no
longer JOINs `code.code`; it returns integer ids only, and the planner
resolves the handful of distinct ids per rule once via the new
`get_codes`, doing set-membership per task instead of a per-task source
compare. This collapses the 256× amplification. (The implementation chose
FK-by-id over the `code_hash` digest sketched here — same effect on the
JOIN, and it keeps the old source recoverable by id so `why` retains its
before→after messages; see the *Implemented* note in
[graduated_discussion.md](../graduated_discussion.md).)

## Issue 3 — `task.uses_hash` inflates the DB (size, not query time)

A distinct problem surfaced from the same investigation: `task.uses_hash`
stores the full serialised-AST of a rule's `uses=` members inline on
every task row, byte-identical across the rule's tasks (~145 KB/row for
`compare_delta_z_candidates`; the wescon-tools DB is **272 MB** for 3341
tasks and 149 KB of distinct code). This bloats the DB and the page-cache
footprint — which amplifies Issue 2's run-to-run *variance* (a bigger
file is slower to warm) — but it is **not** what the status query reads.
Tracked in full under "Display code changes in `uses` functions" in
[graduated_discussion.md](../graduated_discussion.md) (*Measured in the wild*).

**Fix (implemented 2026-07-02, commit 4407d34).** `task.uses_hash`/
`io_hash` inline strings became `uses_code_id`/`io_code_id` FKs into the
content-addressed `code` table (interned find-or-insert), so the planner
compares ints per task and each distinct string is stored once, not once
per task. The in-place migration backfills the FKs, drops the old
columns, and `VACUUM`s — the 272 MB recovers on first contact. (Stage B,
a `rule_uses` per-helper raw-source table for readable diffs, is a staged
follow-up in `todos.md`.)

## Why file this

None is a logic bug — `info` prints the right numbers. But a
read-only status command on a 3k-task pipeline spending ~6.7 s in the DB,
half of it re-fetching data it just fetched, is a real UX cost on a
command run constantly during development. Issue 2 (the `code.code` JOIN,
256× byte amplification) and Issue 3 (`uses_hash` bloat) were both fixed
in commit 4407d34; Issue 1 (the duplicate `get_tasks_status` per
invocation) remains the open plumbing item (~2.7 s).

## Fixes

1. **De-duplicate** *(open).* Have `info` consume the status already
   computed by the planning pass (or, if `info` intentionally skips
   planning, don't also run the planner) — one `get_tasks_status` per
   rule per invocation, not two. Cheaper now that each query no longer
   drags `code.code`, but still redundant work.
2. **Drop the `code.code` JOIN** *(done — 4407d34).* `get_tasks_status`
   returns ids only; the planner resolves the few distinct ids per rule
   via `get_codes` and compares by id. Collapsed the 256× amplification.
3. **Move inline `uses_hash`/`io_hash` off task rows** *(done —
   4407d34).* Now `uses_code_id`/`io_code_id` FKs into the interned
   `code` table; migration backfills, drops the old columns, and
   `VACUUM`s. Stage B (`rule_uses` per-helper raw source for readable
   diffs) is a follow-up in `todos.md`.

Reproducer: any large pipeline; this one is
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py` with a populated
`.remake/` (3031 recorded tasks). Run `remake -D info -F <file>` and read
the paired `get_tasks_status` / `plan` lines from the `-D` log.

## Field verification of the migration (2026-07-02)

Ran the in-place migration (4407d34) against the same wescon-tools
`.remake/remake.db` that produced the measurements above (3341 tasks, all
`success`), by invoking `remake info` under the migration-carrying build.
Backed the DB up first, then checked the result against that backup.

- **Size: 272,154,624 → 987,136 bytes** (260 MiB → 964 KiB) — ~275×,
  99.6% reclaimed. The migration fired exactly once (INFO `Migrating
  task.uses_hash/io_hash to code-table FKs` → `Vacuuming`), and is
  idempotent: a second `remake info` logged no migration and left the
  size unchanged.
- **Schema:** `uses_hash`/`io_hash` dropped; `uses_code_id`/`io_code_id`
  FK columns present.
- **No data loss:** 3341 tasks before and after, all `success`;
  `PRAGMA integrity_check` = `ok`; `PRAGMA foreign_key_check` empty.
- **FKs populated & interned correctly:** 0 NULL `uses_code_id`/
  `io_code_id`; `code` grew 74 → 97 (= the 11 distinct `uses` + 12
  distinct `io` strings folded in, none pre-existing); per-rule
  `distinct uses_code_id = 1`, matching the pre-migration
  `distinct uses_hash = 1`.
- **Content check vs backup (strongest):** joining migrated ⋈ backup by
  task key, `code.code[uses_code_id]` equals the original `uses_hash` and
  `code.code[io_code_id]` the original `io_hash` for **all 3341 tasks — 0
  mismatches**. The FKs resolve to the exact original strings.
- **No mass rerun:** post-migration `remake info` shows **0 to run**
  across all 12 rules (3341/3341 `success`). The FK-by-id design
  preserves unchanged-ness — `_ensure_rule` re-interns the current
  `uses`/`io` strings, which content-match the migrated `code` rows, so
  identity holds and nothing is judged stale. (Verified with the pipeline
  already settled against unrelated edits, so the 0-to-run is attributable
  to the migration alone.)

**Speed (Issue 2 fix confirmed).** `remake -D info` on the same DB, before
the migration vs after (the pre-migration figures are the `-D` timings at
the top of this doc):

| metric | before | after | speedup |
| --- | --- | --- | --- |
| status query, 1465-task rule (planning pass) | 2.218 s | ~0.020 s | ~110× |
| status query, 774-task rule (planning pass) | 0.550 s | ~0.014 s | ~39× |
| `plan()` total | 3.751 s | ~0.70–0.82 s | ~5× |
| **end-to-end wall** | **~10.2 s** | **~4.3 s** (4.32/4.14/4.49) | **~2.4×** |

The per-query cost is now ~0.1 s total across all rules (down from
~6.7 s), and the >1000× run-to-run variance is gone — the tiny DB stays
warm. The residual ~4.3 s wall is now dominated by Python startup +
remakefile import (~2.1 s) and the planner's non-query work, **not** the
DB. Issue 1 (the duplicate `get_tasks_status` pass) is still present in
the log but now costs ~0.05 s, so it is no longer worth chasing on
performance grounds alone.
