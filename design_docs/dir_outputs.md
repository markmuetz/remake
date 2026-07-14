# Directory outputs (`Dir` token)

> **Status: design agreed in discussion (MM + Claude, 2026-07-14); not yet
> implemented.** Target release: 0.9.0 (new API — barred from the 0.8.x
> patch lane by [compatibility.md](compatibility.md)); folded into the
> scoped 0.9.0 alongside resource capture and output validation, with which
> it dovetails ([future_releases/v0.9.0.md](future_releases/v0.9.0.md)).
> Class: **Design** — decisions below are settled unless marked open.

## Motivation

A task can produce a *variable* number of output files — a plot per
detected event, a tile per region that had data — which today can't be
declared as outputs at all (the outputs spec is a fixed dict). The ask
(MM, 2026-07-14): mark a **directory** as produced by a task. All files in
that directory are attributed to that task; each directory is unique to
one task. Primarily bookkeeping — handle variable file counts, and answer
"how much disk does this rule's output use?".

## Design

### 1. `Dir` token

New `class Dir(PathToken)` in `core/tokens.py`, declared like any output:

```python
outputs={'plots': Dir('data/plots/{year}')}
```

Path-backed and `os.PathLike`, so rule code writes into it directly. The
existing architecture does the heavy lifting:

- **Dependencies need no path matching.** There is no task-level DAG;
  downstream linkage is explicit rule-level `depends_on`. "Which task
  reads which file inside the dir" never needs answering for correctness.
  Variable downstream consumption pairs with `@deferrable` matrix
  callables (enumerate the dir once the upstream is complete) — the
  headline use case, worth a new example (ex13).
- **Completion checking is already token-polymorphic** (`is_complete()`;
  `ZarrStore` is precedent for a directory-shaped token with its own
  completion marker).

### 2. Completion = manifest file, not bare existence

`is_complete()` must not be `is_dir()` — a task killed mid-write leaves a
directory that looks complete, and `check_outputs=fallback` /
`set-state --check-outputs` would adopt half-finished output. Instead:
after the run function returns successfully, remake scans the dir and
writes a hidden **manifest** (`.remake-manifest.json`) inside it.
`is_complete()` = manifest exists.

The manifest is built empirically — remake cannot observe writes (tasks
are arbitrary Python), so it records what it finds: a recursive `os.walk`
after success, capturing each file's relative path, size and mtime, plus
aggregates (file count, total bytes). "Scan and trust" is the same
epistemology as `check_outputs`: ask the world, not the DB. Dot-prefixed
remake files (the manifest itself) are excluded from the scan.

Per-file detail stays in the manifest **in the dir**; only aggregates go
to the DB (see §5) — per the "DB stays small" lesson from the 2026-07
storage rework (272 MB → 964 KB).

### 3. Rerun semantics: delete the manifest, never the files

**Decided (MM, 2026-07-14): remake never deletes user files.** An earlier
draft had remake clear a manifest-owned dir before rerun; rejected — a
crashed rerun would destroy the *previous good* outputs, and a
repeatedly-failing task would leave nothing. Instead, at task start remake
deletes only `.remake-manifest.json`: the dir is incomplete while the task
runs, complete when the new manifest lands. Same logical semantics, no
destruction, and a mis-declared `Dir('data')` can't eat anything.

The cost is **stale files** — leftovers from a previous run (or
pre-existing contents) would silently corrupt "all files here came from
this task". Recovered without deletion by *attribution*:

- The task process records its own start time; the post-run scan
  partitions files into `produced` (mtime ≥ task start, or name appeared
  during the run per a pre-run name snapshot) and `stale` (everything
  else), both recorded in the manifest.
- A `stale` entry triggers a warning at task completion and is visible in
  `task-info`. Disk accounting reports both totals.
- Cleanup stays a human decision (a list/remove-stale subcommand can come
  later if it itches).
- Both timestamps come from the same process on the same node, so NFS
  clock skew is not an issue.
- **Known limitation** (document it): tools that preserve source mtimes
  (`shutil.copy2`, `rsync -a`) make genuinely-produced files look stale.
  The pre-run name snapshot covers new names; a copied file *replacing* an
  existing name stays ambiguous and lands in `stale` with the warning.

A non-empty dir with no manifest on first run is **not** an error: its
files land in `stale` with the warning. Adoption via
`set-state --success --check-outputs` is then "scan and claim everything
present" — write a manifest with all files as `produced` — pleasingly
consistent with the file-output adoption path.

### 4. Validation

- **Plan time (cheap, always on):** error if two tasks declare the same
  `Dir` identity (set membership — free at the 1e4-task design scale).
- **`remake lint` only (O(n log n) prefix scans over ~1e6 paths — too
  costly for every plan, per the scale target):** dir nested inside
  another task's dir; one task's file output inside another's dir; a `Dir`
  equal to or containing any declared input of another rule.
- **`lint` fix required:** inputs pointing inside another rule's `Dir`
  won't match the `producers` path map (`core/remake.py` `lint`) and would
  false-positive as `external`/`missing_dependency` — lint needs prefix
  matching against dir outputs.

### 5. DB: aggregate stats

New nullable, additive table (the `_add_missing_columns` forward-compat
pattern):

```sql
output_stat(task_id, name, n_files, total_bytes)
```

Populated for `Dir` outputs at completion (and cheaply for plain file
outputs while we're there). Surfaced in `task-info` and as an aggregate in
`info`. Under SLURM the scan runs in the remote task process, so the
**sidecar format gains the stats** and ingest writes them — moderate,
well-trodden touch.

### 6. Interactions and remaining pitfalls

- **Adopting existing remakefiles:** converting a rule's outputs from N
  files to a `Dir` changes its outputs code → io-change rerun trigger →
  whole rule reruns. Documented migration: edit, then
  `remake set-state -Q ... --success --check-outputs` to restamp and write
  manifests. **Verify during implementation that set-state restamps
  `io_code_id`**; make the recipe a tested, documented path.
- **`ZarrStore`:** already a directory token with its own completion
  marker — document that you don't wrap one in `Dir`.
- **NFS cost:** one walk per task at completion is fine (dentries hot —
  the task just wrote them); `is_complete()` is ~two stats.
- **Upgrade safety:** schema addition must be tested against an existing
  0.8.x DB with **no mass rerun** (same promise 0.8.1 made).

## Implementation order

1. `Dir` token + manifest write/scan/read (incl. mtime/snapshot
   partition).
2. Task-run path: mkdir, manifest delete at start, scan + write at
   success.
3. Plan-time duplicate check.
4. Schema (`output_stat`) + sidecar/ingest for SLURM.
5. `set-state --check-outputs` adoption (scan-and-claim).
6. lint prefix matching + new lint checks.
7. CLI surfacing (`task-info`, `info` aggregate; stale warnings).
8. Example ex13 (`Dir` + `@deferrable` downstream), docs page, tests
   (crash-partial, stale attribution, adoption, upgrade-no-rerun).

## Release

0.9.0, per the scoped plan: design pass done (this doc), minor-release
gates apply — `/code-review ultra` pre-tag, upgrade-no-mass-rerun test.
