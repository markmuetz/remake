# Roadmap

The plan beyond 0.8.0. **Directional, not a commitment** — milestones and
contents will move; each feature still needs its own design pass (the detail
lives in [discussion.md](discussion.md), [future_releases/](future_releases/)
and [todos.md](todos.md)). Compatibility rules for everything here are in
[compatibility.md](compatibility.md).

## Positioning — what we are optimising for

remake's niche is **large task graphs + large file counts + SLURM + content/
AST-aware stale rebuild, in pure Python**. Roadmap items earn their place by
*deepening that niche*, not by chasing general workflow-tool parity. The
standout bets:

1. **Provenance & reproducibility at scale** — make "reliably recreate any
   output" true end-to-end (RO-Crate, env capture, git hash, run report).
2. **remake-as-a-library** — the programmatic `Remake` API (shipped in 0.8.0)
   driven/introspected from a notebook is a real differentiator vs CLI/DSL-
   centric tools; treat it as a headline, and as the backend for everything
   else (CLI, report, web).
3. **Storage-backend tokens as first-class dependencies** — remote artefacts
   as real upstream/downstream deps, which `make` fundamentally cannot do.
4. **Scale ergonomics** — `temp()` intermediates and resource budgets, because
   disk/memory pressure is the actual pain at 1e6 files.

Discipline preserved: **no orchestrator daemon, no passive dashboard.** Static
reports + a queryable DB. (The one deliberate revisit is the *interactive*
web control plane in 0.12.x — see below.)

## Milestones

### 0.9.x — observability & correctness
The scoped slice in [future_releases/v0.9.0.md](future_releases/v0.9.0.md):
output validation (`ensure`-style on the token ABC), per-task resource capture
(wall time + peak RSS, all executors), static DAG/rule-graph export, the
single-file HTML run report, and execution profiles (a shipped `jasmin`
profile).

### 0.10.x — provenance & reproducibility (capture + export)
Bundle the provenance *capture* with the *export* that consumes it, so the
crate ships rich from the start. **Capture:** environment (conda/pip/uv
lockfile or hash per run), pipeline git hash/status per run, optional run-time
output checksums (the shared capability in
[rocrate_export.md](rocrate_export.md)), plus `remake stats` run-history and
query-by-status (`-Q 'status == "failed"'`). **Export:** **RO-Crate export**
([rocrate_export.md](rocrate_export.md)) — a Workflow Run Crate serialised from
the metadata DB; moved here from 0.9 (2026-06-23) precisely so the `agent` /
environment / git / checksum fields it wants are being recorded rather than
omitted. Also clears scale-debt that gates everything (see Cross-cutting).

### 0.11.x — extensibility
Open the abstractions up: storage-backend tokens (generalise `OutputToken` so
S3/GCS/HTTP are *declared* deps, not just `is_complete()` probes), plugin
entry-point discovery (third-party executors / tokens / backends), and the
three-level config cascade with named, shareable profiles generalised from
0.9's `jasmin` profile.

### 0.12.x — interactive web control plane + 1.0 freeze (pre-1.0 capstone)
Two strands, landing together as the last pre-1.0 milestone:

- **Interactive single-page web interface** *(exploration — reverses the
  long-standing "out of scope" stance; full open questions in
  [discussion.md](discussion.md)).* A browser control plane that launches/
  cancels runs, shows task state in real time, drills into failures, and
  re-runs/`set-state`s selections — built as another *render + drive* client
  over the `Remake` API. A genuine differentiator; gated on resolving the
  "live server vs detached-SLURM batch tool" tension first.
- **Toward 1.0 — the freeze.** `temp()`/scratch intermediates and local
  resource budgets/task weights (the scale-ergonomics work), then *stabilise
  the surfaces*: freeze the remakefile DSL, settle the on-disk format, and
  declare the public Python/CLI API per [compatibility.md](compatibility.md).

### 1.0 — the contract begins
Ships once the remakefile DSL, public API and on-disk format are judged
stable. From here, [compatibility.md](compatibility.md) is binding: SemVer,
deprecation ramps, automatic on-disk migrations.

## Cross-cutting (continuous, not a milestone)

Scale-debt from [todos.md](todos.md) that must keep pace with the feature
work — recalibrated 2026-07-02 against the stated scale target (~1e4 tasks ×
~1e2 files/task = 1e6 *files*; see remake3_design.md "Scale target"): the
file-side stat/resolution frontier (1e6-path sweeps on Lustre/NFS), making
`bench_field_scale.py` load-bearing in CI (the 1e6-task bench stays as manual
stress headroom), bounding `retry_lock_commit`, zarr v3 `is_complete()`, and
the long-promised Hypothesis property tests. (Batching the per-task
`EXCLUSIVE` commit was deprioritised — seconds at the design scale.)

## Explicitly *not* doing

Per the design doc and reaffirmed here: orchestrator daemon (rejected as
load-bearing), passive read-only dashboard (low value — query the DB), and
dask-*native* integration (parked; dask misbehaves on JASMIN, remake's target).
The 0.12.x web interface is *interactive*, opt-in, and API-backed — a
deliberate exception argued on differentiator grounds, not a reversal of the
"stay lean" discipline.
