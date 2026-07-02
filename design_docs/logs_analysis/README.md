# remake3 log analysis

Mined from the `remake.log` files of the live remake3 project dirs under
`~/projects` on JASMIN. Generated 2026-07-02.

> **Attribution.** Analysis and CSV extraction by Claude (Opus 4.8) via Claude
> Code, at MM's direction, 2026-07-02. Source data is the field `remake.log`
> files on JASMIN; the causal analysis in §1 was corrected after MM pointed out
> that the cost tracks previously-run tasks needing a code check, not I/O cache
> state — since verified against the source (`sqlite3_backend.py:248`,
> `planner.py:201`/`:358`). Figures are point-in-time snapshots; re-run the
> extraction to refresh.

## Files

- `status_query_timings.csv` — one row per `get_tasks_status` call
  (`project, timestamp, ntasks, nchunks, nfound, seconds`). 1859 rows.
- `planner_timings.csv` — one row per planner summary line
  (`project, timestamp, nrunnable, ndeferred, seconds`). 149 rows.

Source logs with meaningful history: `wescon-tools` (active, 2851 lines),
`mcs_prime_stoch_trigger` (56.8k lines), `hk26_tracking` (284 lines). Other
remake3 dirs had near-empty logs and contribute little/nothing.

---

## 1. Headline findings

### 1.1 `get_tasks_status` is the dominant planner cost, and it scales with previously-run tasks

`get_tasks_status` (`sqlite3_backend.py:248`) is a single
`SELECT ... FROM task LEFT JOIN code ON task.run_code_id = code.id` that pulls
back, **for every task**, the full stored run source (`code.code`). The planner
then feeds that straight into `CodeComparer()(rec.run_code, run_src)`
(`planner.py:201` and `:358`) to decide whether the task's code changed.

So the cost is driven by **how many of the queried tasks have previously run**
(and therefore have stored code to fetch and compare) — *not* by OS I/O cache
state, as an earlier draft of this note wrongly claimed. Cache state only
explains the run-to-run *variance* of an otherwise identical query (see §1.3).

Measured distribution (all projects, 1859 calls):

| percentile | seconds |
| --- | --- |
| p50 | 0.006 |
| p90 | 0.426 |
| p99 | 3.579 |
| max | 10.198 |

Total wall-time spent in `get_tasks_status` across the logs: **363 s (6 min)**.

Per-size means (wescon-tools, the largest DAG):

| ntasks | calls | mean s | max s | mean ms/task |
| --- | --- | --- | --- | --- |
| 216 | 57 | 0.206 | 0.651 | 0.95 |
| 774 | 158 | 0.735 | 3.770 | 0.95 |
| 1465 | 72 | 2.938 | 10.198 | 2.00 |

The ms/task roughly doubles from 774 → 1465 tasks, consistent with the query
returning a per-task code payload (bigger DAGs tend to touch bigger rules).

### 1.2 The code JOIN re-materialises the same source thousands of times

The `code` table is content-addressed and **tiny**, but the status JOIN drags a
full copy of the source for *every* task, so the bytes marshalled per query are
enormous relative to the distinct code:

| project | tasks | run-tasks | distinct code rows | distinct code bytes | bytes fetched by full JOIN | amplification |
| --- | --- | --- | --- | --- | --- | --- |
| wescon-tools | 3341 | 3341 | 74 | 149 KB | **38.4 MB** | **256×** |
| mcs_prime_stoch_trigger | 1039 | 1039 | 94 | 91 KB | 1.6 MB | 17× |
| hk26_tracking | 45 | 45 | 9 | 2.9 KB | 42 KB | 14× |

Every previously-run task hauls ~2 KB of source text through SQLite and into a
Python `TaskRecord`, even though there are only ~74 unique code strings. This is
the concrete mechanism behind the slow status queries — and it is pure waste,
because the planner only needs the full text when the code has *actually
changed* (to render a diff); otherwise a hash comparison would do.

### 1.3 Run-to-run variance is huge (this is the cache effect)

For the *identical* 1465-task query on wescon-tools: **min 0.009 s, median
2.17 s, max 10.198 s** (n=72). A >1000× spread for the same SQL. This is the OS
page cache over a bloated DB file warming and cooling between invocations — it
sits on *top* of the per-task payload cost from §1.2, it does not cause it.

### 1.4 Planner end-to-end can reach 20 s doing zero work

`planner_timings.csv`: 15 of 149 planner runs took >5 s; the slowest was
**20.0 s for a `0 runnable, 0 deferred` plan** — i.e. 20 s spent entirely in
status/DAG/code-compare to conclude nothing needs running.

### 1.5 The DB file is disproportionately large

wescon-tools `remake.db` is **272 MB** for 3341 tasks and only 149 KB of
distinct code. The size is not explained by the code table; it points at
`uses_hash`/`io_hash` storage and/or SQLite free-page bloat and is worth a
`VACUUM` + a look at what those columns actually store. (Cross-ref the
`uses_hash` bloat work in `design_docs/bugs`.)

---

## 2. What else the logs record (for future mining)

- **Subcommand usage** (from `argv:` DEBUG lines): `run` 82, `info` 76,
  `set-state` 14, `ls-tasks` 10, `task-info` 7, `why` 6, `lint` 4, `rule-dag` 3,
  `task-log` 1. Real-world weighting of which commands to keep fast.
- **Schema migrations**: `_add_missing_columns` lines log live upgrades
  (`io_hash`, `run_seq`, `rule.remakefile`, `meta` table) applied to existing
  DBs — a timeline of schema evolution per project.
- **Failure modes**: repeated task failures (e.g.
  `analyse_compare_rhis_to_radarnet[case=20230803]` failed 14×), the
  `sbatch: Please specify an account!` footgun, `matrix not ready — skipped`
  lint warnings.
- **Sidecar ingestion** counts per `info`/`run`.

---

## 3. Reducing log bloat

`remake.log` is already rotated (`remake_cmd.py:843`: `rotation='5 MB',
retention=3`, level DEBUG, or TRACE under `-T`), so size is bounded — the
concern is signal-to-noise within that budget, not unbounded growth. The 2.5 MB
`mcs_prime_stoch_trigger` log is **~55k of its 56.8k lines** from a single
source: `TRACE`-level `code_compare` dumps that print the **entire body of both
function versions** (`code1:` / `code2:`) on every comparison
(`remake.util.code_compare`). Recommendations, roughly in priority order:

1. **Don't log full source at TRACE.** Log a one-line summary
   (`code_compare: <rule> unchanged` / `changed (N lines differ)`), and gate the
   full-body dump behind an explicit opt-in (e.g. `REMAKE_LOG_CODE=1`). This
   alone removes ~95% of the largest log's volume — and because the file is
   capped at 5 MB, cutting the noise means rotation keeps *more useful* history
   in the retained window instead of evicting it.
2. **Split into a separate debug log, keeping DEBUG as the default.** Per MM's
   preference, `remake.log` stays at DEBUG. Add a second sink so the two streams
   don't compete for the 5 MB window: keep the human-facing `remake.log` at
   INFO+ (INFO/WARNING/ERROR — the run narrative) and route the DEBUG/TRACE
   firehose (timing lines, code dumps) to a separate rotated `remake.debug.log`
   that is on by default at DEBUG. Same information retained, but the everyday
   log stays readable and the debug stream can rotate independently.
3. **Demote the per-query timing line** (`get_tasks_status ... in Xs`) to TRACE,
   or emit it only when the query exceeds a threshold (e.g. >100 ms). Right now
   1857 of these dominate the DEBUG stream on any real run; under
   recommendation 2 they land in `remake.debug.log` regardless, but thresholding
   keeps even that stream focused on the slow outliers that matter.

## 4. Making logs more easily mineable

The current lines are human-prose and require fragile regex to parse (this
analysis included). To make the timing/telemetry first-class:

1. **Emit a structured sink alongside the human one.** loguru supports
   `logger.add("remake.jsonl", serialize=True)`, giving one JSON object per
   record with `time`, `level`, `module`, `function`, `line`, `message`, and
   any bound `extra={...}` fields. Mining becomes `jq`, not `sed`.
2. **Bind metrics as structured `extra`, not interpolated into the message.**
   e.g. `logger.bind(ntasks=n, nchunks=c, nfound=f, seconds=dt).debug("status query")`
   so `ntasks`/`seconds` are real fields, not substrings.
3. **Tag events with a stable `event=` key** (e.g. `event="status_query"`,
   `event="plan"`, `event="task_failed"`) so a miner can filter on event type
   without matching on module:function.
4. **Stamp a per-invocation run id** (bind once per CLI call) so every line from
   one `remake run` shares an id — lets you group timings by invocation and
   correlate the planner total with its constituent status queries.
5. **Keep the CSV/JSONL of timings in-tree** for regression tracking: a
   `remake` micro-benchmark that replays a large DAG's plan and asserts
   status-query time stays sub-linear would catch regressions the field logs
   currently only reveal after the fact.

---

## 5. Suggested code follow-ups (beyond logging)

> **Both done 2026-07-02, commit 4407d34** (`perf(metadata): task rows carry
> code-table FKs, not inline text`). Kept below for the record; see bug 04 and
> the *Implemented* note in `discussion.md`.

- **Stop selecting `code.code` in `get_tasks_status`.** ✅ Done — the query now
  returns integer ids only; the planner resolves the few distinct ids per rule
  via `get_codes` and compares by id, collapsing the §1.2 amplification.
  (Implemented as an FK-by-id rather than the `code_hash` digest sketched here.)
- **`VACUUM`** the large DBs and audit `uses_hash`/`io_hash` storage (§1.5). ✅
  Done — `uses_hash`/`io_hash` became `uses_code_id`/`io_code_id` FKs into the
  interned `code` table; the migration backfills, drops the old columns and
  `VACUUM`s (the §1.5 272 MB recovers on first contact).
