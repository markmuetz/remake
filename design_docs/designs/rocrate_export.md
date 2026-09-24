# RO-Crate export

> **Status: design, not scheduled before 1.0.** Re-planned 2026-09-24:
> export is additive, so it ships after 1.0; the provenance *capture* it
> consumes stays pre-1.0 (checksum capture 0.9, env/git 0.10) so the
> history exists when it lands.
>
> *Milestones and 0.9.0 item numbers in the body are as of writing; the
> 2026-09-24 re-plan ([roadmap.md](../roadmap.md),
> [releases/v0.9.0.md](../releases/v0.9.0.md)) supersedes them where they
> differ.*

Design for `remake ro-crate` — packaging a completed (or partially
completed) pipeline as an [RO-Crate](https://www.researchobject.org/ro-crate/)
for publication, archival and citation. Scheduled for **0.10.x** — bundled
with the environment/git/checksum provenance *capture* it consumes, so the
crate ships rich rather than emitting omitted fields (see
[roadmap.md](../roadmap.md); moved here from 0.9 on 2026-06-23). Graduates the
idea sketched in [discussion.md](../discussion.md) into a concrete design.

## Goal

Emit a standards-conformant crate that records *both* the workflow (the
remakefile + its rules) *and* the record of having run it (which tasks
produced which files, when, with what parameters). The deliverable is a
single `ro-crate-metadata.json` plus, optionally, the data alongside it.

This is a strong fit for remake's scientific-workflow niche: reproducible,
citable provenance for large pipelines, and the natural successor to remake2's
archive feature.

## Key insight — it is a serialiser, not new bookkeeping

remake already *holds* almost everything RO-Crate wants. The work is mostly a
**view over existing metadata**, not new tracking:

| RO-Crate needs | remake already has | source |
| --- | --- | --- |
| workflow source | rule `run`/`inputs`/`outputs` source | `Rule.source` (per-rule); per-task `run_code` in `TaskRecord` |
| task ⇄ files | resolved input/output paths per task | `expand_rule` → `Task.inputs`/`Task.outputs` (tokens) |
| file kind | File vs directory vs remote | token type: `FileToken` / `ZarrStore` / `S3Object` |
| when | completion time | `TaskRecord.timestamp` |
| outcome | success / failed (+ traceback) | `TaskRecord.status`, `.exception` |
| parameters | the task's matrix kwargs | `Task.kwargs` |

So the core is a walk over `rules × tasks × TaskRecord` emitting JSON-LD.

## Target profile

The **Workflow Run Crate** profile — it exists precisely for "a workflow plus
a record of running it". Declare `conformsTo` its profile URI, on top of the
RO-Crate 1.1 context. (Pin the profile version; the WfRC profile is still
evolving.)

## Data mapping (remake → RO-Crate / schema.org)

- **pipeline** → the crate root `Dataset`. The remakefile →
  `ComputationalWorkflow` / `SoftwareSourceCode`, set as the crate
  `mainEntity` (`programmingLanguage` Python). remake itself →
  `SoftwareApplication` with its `version` (the engine that ran the workflow).
- **each rule** → a `HowToStep` in the workflow plus a `SoftwareApplication`,
  carrying `Rule.source['run']` (and `inputs`/`outputs` source). Optionally
  attach the change-detection hashes (`uses_hash`, `io_hash`, run-code hash)
  as `PropertyValue`s for a reproducibility audit trail.
- **each task** → a `CreateAction`: `instrument` = the rule, `object` = input
  `File`s, `result` = output `File`s, `endTime` = `TaskRecord.timestamp`,
  `actionStatus` = `CompletedActionStatus` / `FailedActionStatus` from status,
  `error` = `TaskRecord.exception` (failed only), `identifier` = `task.key`,
  kwargs → a list of `PropertyValue` parameters.
- **each input/output** → a `File` (`contentSize`, `dateModified`, optional
  `sha256`, `encodingFormat` by extension). A `ZarrStore` / multi-file output
  → a `Dataset`; an `S3Object` → a `File` referenced by URL. The token type
  discriminates, so no path-sniffing.

## What is missing, and graceful degradation

The crate must be **valid with whatever is recorded today**, and get *richer*
as the capture features land. Bundling export into **0.10.x** alongside the
provenance *capture* (the reason for the move from 0.9) means the
environment / git / checksum fields are being recorded in the *same* milestone
rather than back-filled later. Fields not stored by 0.8:

| Field | Status | Plan |
| --- | --- | --- |
| `startTime` / wall time / peak RSS | not stored — `TaskRecord` has only completion `timestamp` | filled once **per-task resource capture** (0.9) lands; until then emit `endTime` only |
| `sha256` per file | not stored | the **general stored-checksum capability** (see below) — **capture moved to 0.9.0** (decided 2026-07-14, MM: record in 0.9, consume in 0.10 — export needs the 0.9-era history to exist; rides the `output_stat` migration, see [releases/v0.9.0.md](../releases/v0.9.0.md) item 2); else `--checksums` cold-reread fallback, else omit |
| `agent` (producing user/host) | not stored per task | SLURM sidecars could carry it; otherwise omit (or, weakly, the crate author at export time — *not* provenance-accurate, so prefer omit) |
| environment (conda/uv lock or hash), pipeline git hash | not stored | **0.10 provenance capture**, co-shipped with export — attach to the workflow / `CreateAction`s when present |

Design rule: **never block the crate on a missing optional field** — emit a
conformant Workflow Run Crate, and let provenance completeness scale with the
capture features. So even a crate of a pre-0.10 (0.8/0.9) run stays valid, just
sparser; the 0.10 co-shipping means a *natively-0.10* run is rich from the
first export.

## Checksums — a general capability, not a crate detail

Hashing belongs **at workflow time, on the node that produced the output**:
the bytes are local and hot in page cache, the work is distributed across the
array, and the digest captures the output *as produced* (so later
drift/corruption is detectable). Re-hashing at `ro-crate` time means a cold
serial re-read of the whole tree over the network — or the data has been
purged off scratch and cannot be hashed at all.

So a stored output checksum is a **shared capability** consumed by `verify
--checksum`, content-addressed output-versioning, the stats store and dedup —
RO-Crate is just one reader. **Capture ships in 0.9.0** (2026-07-14: recorded
opt-in at the post-success hook, sha256, `checksum` column on `output_stat`,
per-file in `Dir` manifests; all *readers* stay 0.10.x — checksum-based
staleness in particular is a new rerun trigger and gets its own design pass).
Tri-state (it can't be unconditionally on —
hashing multi-GB netCDF/zarr every run is a tax on everyone):

- **off** (default) — `ro-crate --checksums` stays as the lazy cold-reread
  fallback;
- **on at run time** — opt-in `config={'checksum': 'sha256'}` / `run
  --checksum`: `run_task` streams the output post-success, computes sha256
  compute-side, and ships the digest in the **sidecar payload**;
- **hybrid read** — consumers use stored digests when present, else
  compute-with-warning or omit.

This capability is specced here for context but is **its own work item**, not
part of the RO-Crate deliverable; RO-Crate consumes whatever digests exist.

## API / CLI split

Per the "CLI is a thin render layer over a complete Python API" principle
([compatibility.md](../compatibility.md), `design.md`):

- **`Remake.ro_crate(query=None, *, include_data=False, checksums='auto',
  completed_only=True, summary=False)` → `CrateManifest`** — pure construction
  of the crate: the `ro-crate-metadata.json` dict (the `@graph`) *plus* a
  manifest of file entities (`crate_path`, on-disk source / URL, whether to
  copy). No filesystem writes. This is the data method, unit-testable against
  a golden JSON.
- **`remake/export/rocrate.py : write_crate(manifest, out_dir, *, zip=False)`**
  — the IO half: create the crate dir, write the JSON, copy data (only under
  `include_data`), optionally zip. Side-effecting, kept out of `Remake`.
- **`remake_ro_crate(args)`** (CLI) — parse args → `rmk.ro_crate(...)` →
  `write_crate(...)`. Thin.

### CLI shape

```bash
remake ro-crate [remakefile] [-o DIR] [--zip] [-Q QUERY] \
                [--include-data] [--checksums] [--summary] [--all-tasks]
```

- **reference mode** (default) — metadata-only crate; `File` entities point at
  data in place (relative path or URL). Cheap; for an existing tree. *Not*
  self-contained — paths must stay valid.
- **`--include-data`** — copy outputs into the crate dir / `--zip`.
  Self-contained, for archival/publication; the heavy path.
- **`-Q`** — scope which tasks are crated (see Scale).
- **`--all-tasks`** — include failed/pending tasks too (default
  `completed_only`: only `CreateAction`s for successful tasks, plus failed
  ones carry `FailedActionStatus` when included).

## Scale

1e6 tasks → 1e6 `CreateAction`s is an enormous JSON-LD document. Mitigations:

- **`-Q`-scope is the expected default usage**; a whole-pipeline crate is the
  exception, not the rule.
- **Warn past a threshold** (e.g. > ~50k actions) before emitting.
- **`--summary`** — one `CreateAction` per *rule* (with task count and
  aggregate I/O), trading per-task granularity for a citable, tractable crate.
- **Stream the JSON writer** — emit `@graph` entities incrementally rather
  than building the whole list in memory (consistent with the lazy,
  constant-memory expansion elsewhere in remake).

## Implementation notes

- **Hand-roll the JSON-LD.** The `@graph` is a small list of dict entities we
  fully control; this matches the dep-averse stance elsewhere (cf. the
  Drain3-vs-hand-rolled call). Pull in `ro-crate-py` later *only* if
  validation / round-trip earns it. Either way RO-Crate is an **optional
  extra** (`remake[rocrate]` if a dep is ever used) — never a core dependency.
- **Deferred / dynamic-matrix rules** whose tasks can't be expanded yet are
  skipped with a warning and noted in the crate description as incomplete (you
  cannot crate what hasn't been resolved).
- **Side-effect rules** (no outputs) → `CreateAction` with empty `result`;
  **source rules** (no inputs) → empty `object`. Both still recorded.

## Testing

- Golden `ro-crate-metadata.json` for a small fixture pipeline (linear +
  fan-in + a `ZarrStore` + an `S3Object`), asserting the `@graph` shape.
- Optional CI validation with `ro-crate-py` / a JSON-LD/profile validator,
  behind the `rocrate` extra.
- Property check: every `result` File of a completed task's `CreateAction`
  corresponds to an actual `Task.outputs` token.

## Open questions

- Pin to which Workflow Run Crate profile version, and how to track its
  evolution.
- Default for failed/pending tasks — `completed_only` (proposed) vs include
  all attempts as provenance.
- `contentSize` for `ZarrStore` / directory outputs: stat the whole tree
  (cost at scale) vs omit when `--include-data` is off.
- Whether to surface the internal `run_seq` ordering as provenance, or keep it
  internal (leaning: keep internal).

## Appendix: Origins

The sketch this design grew from, moved verbatim from
[discussion.md](../discussion.md) on 2026-09-24.

- **Integrate RO-Crate** — *graduated to a full design doc:
  [rocrate_export.md](rocrate_export.md) (scheduled 0.10.x, bundled with the
  env/git provenance capture). The notes below are the origin sketch, kept for
  the record.* Package outputs + metadata +
  provenance as an
  [RO-Crate](https://www.researchobject.org/ro-crate/) for
  publication/archival; natural successor to remake2's archive feature.
  Key realisation: remake already *holds* almost everything RO-Crate wants
  (rules, tasks, kwargs, input/output paths, code/uses/io hashes,
  timestamps, status) — the feature is mostly a **serialiser** over the
  existing metadata, not new bookkeeping.
  - *The mapping (remake → RO-Crate / schema.org):*
    - pipeline → the crate root `Dataset`; the remakefile → the
      `ComputationalWorkflow` / `SoftwareSourceCode` `mainEntity` (language
      Python; remake itself a `SoftwareApplication` with its version).
    - each rule → a `HowToStep` / `SoftwareApplication` carrying the rule's
      `run` source (already stored in `rule.source`).
    - each completed task → a `CreateAction`: `instrument` = the rule,
      `object` = input `File`s, `result` = output `File`s, `startTime`/
      `endTime` from metadata, `actionStatus` = Completed/Failed from task
      status, kwargs → `PropertyValue` parameters, `agent` = user/host.
    - each output → a `File` (`contentSize`, `dateModified`, optional
      `sha256`, `encodingFormat` by extension); a zarr/multi-file output →
      a `Dataset`, an S3 output → referenced by URL (the token type already
      tells us which: FileToken / ZarrStore / S3Object).
  - *Target profile:* the **Workflow Run Crate** profile (declare
    `conformsTo` its URI); it exists precisely for "a workflow plus a record
    of running it".
  - *Two modes:* **reference** (default) — metadata-only crate whose `File`
    entities point at data in place (cheap, for an existing tree); and
    **`--include-data`** — copy outputs into the crate dir / zip
    (self-contained, for archival/publication, the heavy path).
  - *Shape:* `remake ro-crate [remakefile] [-o DIR] [--zip] [-Q query]
    [--include-data] [--checksums]`. `-Q` scopes which tasks are crated.
  - *Implementation:* a new `remake/export/rocrate.py` that walks
    rules/tasks/metadata and emits `ro-crate-metadata.json`. **Hand-roll the
    JSON-LD** (the `@graph` is a small list of dict entities we fully
    control) rather than depend on `ro-crate-py` — consistent with the
    dep-averse stance elsewhere (cf. the Drain3-vs-hand-rolled call); pull
    `ro-crate-py` in later only if validation/round-trip earns it. Optional
    extra either way (`remake[rocrate]` if a dep is used).
  - *Checksums belong at workflow time, not crate time (own capability).*
    Hashing should happen **on the node that produced the output, right
    after it is written** — the bytes are local and hot in page cache, the
    work is distributed across the array, and the result is captured *as
    produced* (so later drift/corruption is detectable). Re-hashing at
    `remake ro-crate` time means a cold serial re-read of the whole tree
    over the network — or the data has been purged off scratch and can't be
    hashed at all. So a stored output checksum is a **general capability**,
    not an RO-Crate detail: `verify --checksum` (corruption, not just
    existence), output-versioning **(B) content-addressing**, the stats
    store, and dedup all consume it; RO-Crate is one reader. Tri-state, since
    it can't be unconditionally on (hashing multi-GB netCDF/zarr every run
    costs everyone): **off** (default; `ro-crate --checksums` stays as the
    lazy cold-re-read fallback), **on at run time** (opt-in
    `config={'checksum': 'sha256'}` / `run --checksum` → `run_task` hashes
    outputs post-success and ships the digest in the **sidecar payload**,
    computed compute-side), and a **hybrid read** (consumers use stored
    digests when present, else compute-with-warning or omit). sha256 for
    crate conformance; streamed post-write read rather than wrapping the
    write handle.
  - *Sharp edges:* **Scale:** 1e6 tasks → 1e6 `CreateAction`s is an enormous
    JSON-LD — so `-Q`-scope by default, warn past a threshold, and offer a
    `--summary` mode that emits one `CreateAction` per *rule* (with a task
    count) instead of per task. Incomplete/deferred tasks are omitted (or
    marked); reference-mode `File`s may be absent on scratch (degrade like
    `check_outputs`).
  - *Relates to:* the **stats store** (richer `CreateAction` timing /
    agent / parameters when present — but degrade to `last_run_timestamp`
    when not) and **grab code version / python module state** (workflow
    provenance: git hash + env → `SoftwareSourceCode.version` /
    environment). Degrade gracefully when those aren't recorded.
