# Bug 01 — Upstream→downstream rerun propagation is not durable

**Status:** fixed (run-sequence id, option 2 below) — 2026-06-22.
**Affects:** local and SLURM execution.
**Moved from** `design_docs/discussion.md` (was "Propagation gap").
**Reported by:** Mark Muetzelfeldt — identified the partial-target (`run -Q`)
failure case (scenario 2) and the A→B→C cascade consequence for
`set-state --success` that motivated the guarded cascade.

**Implemented:** `run_seq` column + `meta` counter (sqlite3_backend.py),
`begin_invocation`/`current_run_seq` allocation (one per `run`/`set-state`),
sidecar + SLURM job-spec threading, planner durable check
(`_max_upstream_run_seq`), `remake why` `upstream-newer` reason, and
guarded cascade-by-default `set-state --success` / `--no-cascade`
(`cascade_settled`). Tests: `test_partial_target_rerun_propagates_to_downstream`
(regression, integration), `test_durable_propagation_after_partial_upstream_run`,
`test_explain_reports_upstream_newer`, `test_cascade_settled_*` (unit),
`test_why_upstream_newer_after_partial_target`,
`test_set_state_cascade_settles_downstream`,
`test_set_state_no_cascade_leaves_descendant_stale` (CLI), plus the
run_seq round-trip in `test_run_array_task_writes_sidecar_not_db`. Docs:
SKILL.md (rerun reason 8, set-state), docs/cli.md, docs/guide/debugging.md.

## The bug

The planner's "an upstream is rerunning → rerun the downstream" signal is
**ephemeral** — it lives only inside a single `plan()` pass (`rerun_kwargs`,
planner.py), computed in one topological sweep, and is never persisted. A
task's stored `TaskRecord` carries `status`, `timestamp`, `run_code`,
`uses_hash`, `io_hash` — but *nothing about its upstream's state*. So the rule
is really "B reruns if A is rerunning **in the same pass**", which only holds
if A and B rerun together atomically. Anything that runs A without B in the
same pass strands B; a crash is only one way to get there.

### Scenario 1 — crash

Both A and B succeeded earlier (B consumed A's old output). Edit A's code,
`remake run`: the plan flags A (code-changed) and B (upstream-reruns), ordered
A-then-B. A runs and commits `SUCCESS` with the new code hash; **the process
dies before B runs** (kill, OOM, node loss; under SLURM: A's array job
succeeds, B's is cancelled). B's record is untouched — still `SUCCESS` from the
first run. On the *next* `remake run`, A is now up-to-date (its stored code ==
current), so A is **not** rerunning this pass; B's own code/`uses`/`io_hash`
are unchanged → B is judged up-to-date and **never reruns**, silently keeping a
result built from A's stale output. This is the flip side of rejecting mtimes:
robust against scrambled clocks, but not crash-atomic across the
producer→consumer edge.

### Scenario 2 — partial target (no crash, no failure)

Same starting state (A and B both `SUCCESS`, B built from A's old output). Edit
A's code, then deliberately run only A: `remake run -Q "rule == 'A'"`. The
query filters B out of the plan entirely, so A reruns and commits `SUCCESS`
with the new code hash and new output, while B is never even considered — the
in-pass propagation signal that would have flagged B is never produced. On the
next *unfiltered* `remake run`, A is up-to-date (stored code == current) so it
doesn't rerun this pass, B's own hashes are unchanged → B is judged up-to-date
and **never reruns**, again silently serving a result built from A's superseded
output. No crash, no SLURM, no failure — just ordinary, documented `-Q`
targeting (the skill even bills `--force -Q` / `run -Q` as the "surgical rerun
tool"). This is the more alarming case: scenario 1 needs a process death, but
this is reachable on a healthy single-host run by following the recommended
workflow. It also sharpens the existing "fix-one-failure idiom" caveat —
downstream tasks excluded by a query don't merely "stay unrun until a later
unfiltered run", they can stay *stale-but-marked-success forever*, because once
A is up-to-date the later unfiltered run has nothing left to propagate from.

Regression test: `test_partial_target_rerun_propagates_to_downstream`
(tests/integration/test_pipeline.py).

## Fix options (the durable signal must be persisted, not recomputed)

### (1) DB-stored execution time (make's model, trustworthy clock)

The design rejected *file mtimes* because the *filesystem* sets them and
rsync/tar/Lustre rewrite them — **not** timestamps as such. A timestamp remake
writes into its own DB on success is a clock we control. Add a plan check: B
reruns if `max(upstream.timestamp) > B.timestamp`. Reuses the existing (today
display-only) `timestamp` field. In both scenarios A=t3 > B=t2 → B reruns.
Cheap, durable, survives process boundaries *and* partial-target runs.

- *Caveat — granularity.* Both write paths store **whole seconds**
  (`datetime('now')`, sqlite3_backend.py; `strftime('%Y-%m-%d %H:%M:%S')`,
  sidecar.py). Fast tasks tie within a second: strict `>` misses a real "A
  after B" inside one second, `>=` causes spurious reruns on every tie. Needs
  sub-second resolution before timestamps can *order* rather than merely
  *display*.
- *Caveat — cross-node clocks (SLURM).* The sidecar timestamp is `time.gmtime()`
  on whatever compute node ran the task, so under SLURM we'd be comparing A's
  clock (node X) against B's (node Y) — making correctness depend on inter-node
  NTP sync, exactly the environmental assumption remake otherwise avoids. Fine
  locally (single writer, one clock); only partly "in our control" distributed.

### (2) Logical clock / run-sequence id (clock-independent (1)) — CHOSEN

Assign a monotonic run/commit sequence number at launch and thread it through
the SLURM job spec into the sidecar, alongside (or instead of) wall-clock. B
reruns iff `max(upstream.run_seq) > B.run_seq`. Tie-free, no node-clock or
sub-second dependence — arguably the most in-our-control form of (1), since the
sequence is assigned by remake at submission time, not read from any clock on
the node.

### (3) Persisted upstream-provenance hash

Store in B's record a hash of its upstream provenance at build time (the
producing tasks' `run_code`, or a content hash of B's actual inputs). Plan
check: recorded upstream-hash ≠ current → rerun. *Precise* — skips B when A's
rerun produced byte-identical output (which (1)/(2) would conservatively rerun,
like make) — and a content hash doubles as corruption detection. Cost:
storing/comparing the extra hash, and content-hashing possibly-huge outputs
(ties into the output-checksum capability under *Integrate RO-Crate* and the
stats store).

### Relationship

(1)/(2)/(3) all make the propagation signal **durable**, which closes *both*
scenarios above (crash and partial-target) with the same mechanism — a
persisted "upstream is newer than me" check that no longer depends on A and B
being planned in the same pass; none replaces the existing
`run_code`/`uses_hash`/`io_hash` checks (those catch "recipe changed" with no
upstream run) or in-pass propagation (still wanted so a single `remake run`
does A and B in one go). (1)/(2) are the cheap, conservative correctness fix
(rerun-if-newer, may over-rebuild); (3) is the precise but heavier option.
Leaning: a **run-sequence id (2)** is the smallest change that is correct under
both local and SLURM execution without depending on node clocks or sub-second
timestamps; ship that to close the gap, and treat (3)/content-addressing as the
later precision + corruption-detection upgrade. Relates to the **I/O
verification / reconcile** item (its rejected `--mtime` option is the
*file*-mtime version of this same idea) and the **stats / run-history store** (a
`run_id` per invocation is the natural home for the run-sequence).

## Implementation (option 2)

A single monotonic integer `run_seq` per `remake run` invocation, persisted on
every task record and compared against upstreams in the planner.

1. **Allocation.** One `run_seq` per invocation, allocated by the launching
   process from a `meta` table (`max(run_seq)+1`). One value for the whole run,
   shared by every task it commits — including the SLURM submit, so all array
   elements of one submission carry the same value regardless of node clocks.
2. **Persistence.** Nullable `run_seq INTEGER` column on `task`; field on
   `TaskRecord`; written by `_upsert_task` (local) and `_ingest_records`
   (sidecar) alongside `last_run_timestamp`.
3. **Sidecar path.** The submitting `remake run -E slurm` writes `run_seq` into
   each job spec (`.remake/jobs/<rule>.json`); `run-array-task` reads it and
   `SidecarWriter.update_task` adds it to the payload, exactly as it already
   does for `io_hash`/`timestamp`; `_ingest_records` copies it into the column.
4. **Planner check.** After the existing code/uses/io checks and before "up to
   date", B reruns if `max(upstream_rec.run_seq) > B_rec.run_seq`, using the
   same `_same_matrix` element-wise-vs-conservative dependency selection. A
   `run_seq_by_task[rule]` dict is threaded in topo order, mirroring
   `rerun_kwargs`. In-pass propagation is kept; this is the cross-pass backstop.
5. **`remake why`.** `explain_task` gains a new `upstream-newer` reason
   (distinct from the in-pass `upstream-rerun`) emitted when
   `max(upstream.run_seq) > task.run_seq` but the upstream is not in `runnable`.
   Without this, `why` would report `will_run=True` with zero reasons — the
   silent verdict it exists to prevent. Requires fetching upstream task records
   (their `run_seq`) in `explain_task`.

### Edge cases

- **Migration.** Pre-upgrade rows have `run_seq = NULL`; treat NULL like the
  `io_hash` NULL case — "not yet tracked", never rerun on that alone — so an
  upgrade doesn't trigger a mass rerun. First subsequent run stamps live values.
- **Semantics.** Conservative like make — B reruns whenever A is newer, even if
  A reproduced identical bytes. Byte-precise skipping is option (3), deferred.
- **`set-state`/`--success`.** Those upserts must also stamp the current
  `run_seq`, or an adopted upstream would look older than its consumers. This
  has a non-trivial consequence for mid-graph tasks — see *Guarded cascade*.

## Guarded cascade for `set-state --success`

### Why marking one task is not enough

`set-state --success` allocates the current (highest) `run_seq` and stamps it
onto the selected task, so the task becomes the newest thing in the graph and no
upstream out-ranks it — that *is* "mark complete". But because the stamp is
"now", it also makes the marked task **newer than its own descendants**, which
then look stale.

The following `A → B → C` example, and the realisation that settling a
mid-graph task leaves its descendants looking stale, are due to Mark
Muetzelfeldt.

Chain `A → B → C`, all last run at run_seq 1. Edit A, run only A (`A=2`). You
judge A's change didn't affect B's output and run `set-state B --success`
(invocation run_seq 3 → `B=3`):

- B vs A: `max(A=2) > B=3`? No → B suppressed (intended).
- C vs B: `max(B=3) > C=1`? **Yes → C now reruns** — a *new* spurious rerun
  introduced by settling B.

There is no single stamp for B that both clears A (needs `B ≥ 2`) and avoids
triggering C (needs `B ≤ 1`); `A` is transitively ahead of `C`. The only fix is
to also re-stamp the descendants — i.e. `set-state --success` **cascades**
downstream by default.

### The diamond hazard, and the guard

Naïve cascade over-suppresses a descendant that has a *second*, independently
changed upstream. `D` depends on both `B` and `E`; both `A` and `E` reran
(`A=2`, `E=2`); `B=1, D=1, C=1`:

```
   A=2          E=2          ← both reran
    │            │
    ▼            ▼
   B ──► D ◄──── E           D's deps = {B, E}
         │
         ▼
         C
```

`set-state B --success` cascading blindly would stamp `D=3`, so `max(E=2) > D=3`
is false and **E's real change to D is silently swallowed**. run_seq is one
scalar per task, so a stamp that clears the `B→D` edge unavoidably clears the
`E→D` edge too (per-edge precision is option (3), deferred).

**Guard:** when cascading, re-stamp a descendant **only if none of its
dependencies is newer than it**. `D` has `E=2 > D=1`, so D is skipped — left at
1, so `max(E=2) > D=1` still fires and D correctly reruns.

### Why the guard needs no subtree pruning (it is purely local)

Skipping `D` does **not** require also skipping/pruning `D`'s descendants. `C`
gets stamped to 3 (at set-state time `D=1` is not newer than `C=1`, so C's own
guard does not fire). When `D` later reruns it gets a strictly higher `run_seq`
(monotonic, later invocation), e.g. 4, so `max(D=4) > C=3` re-fires and C reruns
then. A node left un-stamped re-propagates to its own descendants automatically
on the pass where it actually reruns. So the guard is a single, local,
per-node question about a task's immediate dependencies — no region tracking.

### The rule, and its safety property

> Cascade re-stamps a descendant unless one of its dependencies is newer than
> it. A skipped node reruns through normal propagation, which then carries on to
> its own descendants on the next pass.

Mental model: *the new stamp floods downstream but stops at any merge point
where an un-settled change is flowing in.* The guard can only ever cause **extra
rebuilds, never missed ones** — skipping a re-stamp is conservative, matching
remake's no-mtimes / rerun-if-unsure stance. (A crude form, "≥2 deps and any is
newer → skip", can over-rebuild when the newer dep was *also* in the same
`set-state` selection; a precise form, "newer dep *outside* the selection",
removes even that. Either is correct; ship the simple one first.)

### Surface

- **Cascade is the default** for `set-state --success`. A `--no-cascade` flag
  stamps only the selected tasks (descendants then rebuild via normal
  propagation — conservative but correct).
- `--check-outputs` verifies outputs for the **whole** stamped set (selected +
  cascaded), not just the named tasks.
- Cascade only re-stamps descendants already recorded `SUCCESS`; it never
  fabricates completion for never-run or failed tasks.
- The `A → B → C` linear case has single-dep descendants, so the guard never
  fires and the spurious `C` rerun is eliminated; the diamond `D` is protected.

### Implementation note (adds to the list above)

6. **`set-state --success` cascade.** After stamping the selected tasks, walk
   their transitive `SUCCESS` descendants in topological order; re-stamp each
   with the same `run_seq` unless one of its dependencies has a higher `run_seq`
   (the guard). `--no-cascade` skips the walk. `--check-outputs` covers the
   cascaded set. Tests: linear-chain suppression, diamond-guard protection,
   `--no-cascade`, and `remake why` reporting `upstream-newer` vs not after each.
