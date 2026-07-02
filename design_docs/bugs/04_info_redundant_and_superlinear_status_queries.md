# Bug 04 — `remake info` re-queries task status a second time, and the per-rule status query scales superlinearly

**Status:** open (performance) — reported 2026-07-02.
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

**Fix:** stop selecting `code.code` in `get_tasks_status`; store and
compare a `code_hash` (as `run`/`uses` do), and fetch the full source
lazily by `run_code_id` only when the hash differs and a diff is actually
rendered. That collapses the amplification and should flatten the
scaling — query-side, independent of any `uses_hash` rework.

## Issue 3 — `task.uses_hash` inflates the DB (size, not query time)

A distinct problem surfaced from the same investigation: `task.uses_hash`
stores the full serialised-AST of a rule's `uses=` members inline on
every task row, byte-identical across the rule's tasks (~145 KB/row for
`compare_delta_z_candidates`; the wescon-tools DB is **272 MB** for 3341
tasks and 149 KB of distinct code). This bloats the DB and the page-cache
footprint — which amplifies Issue 2's run-to-run *variance* (a bigger
file is slower to warm) — but it is **not** what the status query reads.
Tracked in full under "Display code changes in `uses` functions" in
[discussion.md](../discussion.md) (*Measured in the wild*), whose chosen
fix (demote `uses_hash` to a real digest, move raw source to `code` at
rule granularity) is the `uses` counterpart of the Issue 2 fix above.

## Why file this

None is a logic bug — `info` prints the right numbers. But a
read-only status command on a 3k-task pipeline spending ~6.7 s in the DB,
half of it re-fetching data it just fetched, is a real UX cost on a
command run constantly during development. Issue 1 is a straightforward
plumbing fix (~2.7 s); Issue 2 is the `code.code` JOIN (256× byte
amplification) and has a concrete fix; Issue 3 (`uses_hash` bloat) is a
storage-shape fix tracked in `discussion.md` — all three worsen as
pipelines grow.

## Suggested fix

1. **De-duplicate.** Have `info` consume the status already computed by
   the planning pass (or, if `info` intentionally skips planning, don't
   also run the planner) — one `get_tasks_status` per rule per
   invocation, not two.
2. **Stop selecting `code.code` in `get_tasks_status`.** Store and
   compare a `code_hash` (as `run`/`uses` do), fetching the full source
   lazily by `run_code_id` only when the hash differs and a diff is
   actually rendered. Collapses the 256× amplification and should flatten
   the 774 → 1465 scaling.
3. **Demote `task.uses_hash` to a real digest** (and move raw `uses`
   source to the `code` table at rule granularity) to shrink the DB and
   its page-cache footprint — the storage rework tracked under "Display
   code changes in `uses` functions" in
   [discussion.md](../discussion.md).

Reproducer: any large pipeline; this one is
`wescon_tools/ctrl/remakefiles/wescon_radar_dev.py` with a populated
`.remake/` (3031 recorded tasks). Run `remake -D info -F <file>` and read
the paired `get_tasks_status` / `plan` lines from the `-D` log.
