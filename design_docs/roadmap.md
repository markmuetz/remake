# Roadmap

The plan beyond 0.8.x. **Directional, not a commitment** — milestones and
contents will move; each feature still needs its own design pass (the detail
lives in [discussion.md](discussion.md), [releases/](releases/)
and [todos.md](todos.md)). Compatibility rules for everything here are in
[compatibility.md](compatibility.md).

> **Re-planned 2026-09-24 (MM)** after the full-implementation review
> ([code_reviews/2026-09-24_review.md](code_reviews/2026-09-24_review.md)).
> The previous plan ran four feature milestones (0.9 → 0.12) before 1.0,
> ordered by theme. The new plan asks one question of every item — *does 1.0
> need it?* — and moves everything additive to after 1.0: two milestones
> instead of four, and a smaller frozen surface. The per-item assessment is
> [below](#assessment-of-the-previous-plan-2026-09-24).

## Positioning — what we are optimising for

remake's niche is **large task graphs + large file counts + SLURM + content/
AST-aware stale rebuild, in pure Python**. Roadmap items earn their place by
*deepening that niche*, not by chasing general workflow-tool parity. The
standout bets:

1. **Correct stale rebuild at scale** — the core promise. "Never
   under-rerun, rarely over-rerun" must hold across executors, Python
   versions and real pipeline shapes before anything is frozen.
2. **Provenance & reproducibility** — make "reliably recreate any output"
   true end-to-end (env capture, git hash, checksums; export formats later).
3. **remake-as-a-library** — the programmatic `Remake` API driven/introspected
   from a notebook, as the backend for everything else (CLI, reports, any
   future UI).
4. **Scale ergonomics** — disk/memory pressure is the actual pain at 1e6
   files (`Dir` outputs, scratch/`temp()` lifecycle, file-side stat costs).

Discipline preserved: **no orchestrator daemon, no passive dashboard.** Static
exports + a queryable DB.

## What 1.0 means — the test applied to every item

[compatibility.md](compatibility.md) says 1.0 ships when three surfaces are
stable: the **remakefile DSL**, the **public Python/CLI API** (incl. `--json`
output), and the **on-disk format**. So:

- A feature that **changes one of those surfaces** must be designed (not
  necessarily fully built) **before** the freeze.
- A feature that is **purely additive** — a new command, export, or client —
  can ship in any 1.x and **does not gate 1.0**.
- **Correctness of what gets stored** (hashes, keys, schema) gates 1.0,
  because freezing a defect makes fixing it a breaking change.

## Pre-tag review gate

**Minor releases** (`v0.8.x..v0.9.0`) get an **adversarial review** of the
full diff since the previous tag, before tagging — a fresh reviewer context
that sees only the diff, instructed to find flaws (the Bun-in-Rust lesson:
the implementing context is biased toward approving its own work):
`/code-review ultra` per feature branch/PR, or a per-subsystem agent fan-out
over the tag range if work landed on `main` directly.

**Patch releases** need no pre-tag review (decided 2026-07-10 at 0.8.2 —
mandatory review per point release is unsustainable). The per-commit hook in
`.claude/hooks/` (blocks >200 changed-Python-line commits until reviewed)
plus a manual check that the [compatibility.md](compatibility.md) patch rules
held is enough; run a review only when the range warrants it. (The 0.8.1
pre-tag review did catch two real bugs and a lane violation — for a large or
executor-heavy range, still consider one.)

**Which model reviews** (decided 2026-09-24, MM). Neither the hook nor this
gate requires a particular model — only a fresh, adversarial context — so
match the model to what a miss costs:

- **Per-commit hook reviews** (>200 changed Python lines): **Sonnet**, at
  medium effort, scoped to `src/`. Frequent and narrow, and a miss is likely
  caught by the pre-tag review. Run it as a review agent with a model
  override: `/code-review` runs as a fork of the session and always uses the
  session's model.
- **Pre-tag reviews of a release range**: **Opus**. Once per release, the
  last check before PyPI; the bugs that matter here (e.g. the 0.8.4 M5/M6
  review's masked disk-full `ROLLBACK` and legacy half-created DBs) need
  reasoning about failure modes, not just reading the diff.
- **Haiku**: mechanical checks only (links, CHANGELOG consistency), never
  adversarial review.

## Milestones

### 0.8.x — maintenance lane (parallel, not a milestone)
Non-breaking bug fixes and safe robustness shipped as patches. Patch rules
(no API/DSL/schema breaks, no reruns caused by the upgrade itself) in
[compatibility.md](compatibility.md). **0.8.4** (released 2026-09-24) was the review's patch list:
multiproc/dask `run_seq` (H1), `SystemExit` in tasks (H6), duplicate rule
names (H8), atomic DB create + migrations
(M5, M6), non-zero exit for blocked rules (M9), recording pre-`fn` failures
(M11), tracebacks in per-task logs (M12), query errors/typos (M14), a local
run lock (M15), ingest monotonicity (M17), bad-sidecar quarantine (L14),
colour/BrokenPipe/exit-code hygiene (L25–L27), and the failure-skip part of
H3. Plus the older debts in [todos.md](todos.md) (`retry_lock_commit`, zarr
v3). **0.8.5** follows with worker-crash tolerance (H7) and clean
Ctrl-C/SIGTERM (M10) — deferred 2026-09-24 because their process/signal
tests are the slow, flaky part. Cut from `main` while it is fix-only; branch
from the latest tag if feature work has landed — as of 0.8.4 it has
(resource capture), so the lane is the `maint/0.8.x` branch from `v0.8.3`,
with each fix merged forward to `main`.

### 0.9.x — correctness of the core + DSL shape
Everything that changes what is *stored* or what the *DSL* looks like, so it
is settled before the freeze. Scoped in
[releases/v0.9.0.md](releases/v0.9.0.md).

- **Change-detection correctness** (review H2–H5, M1–M4, M8, M19): strip the
  decorator from run code; canonical, order-insensitive, Python-version-
  independent rendering of `uses`/io values; path-derived task-to-task
  dependencies replacing the shared-matrix assumption (review Appendix A);
  canonical task keys; scope-analysis fidelity; a resolved-path digest for
  fan-ins. Each normalises *both* sides so no existing DB mass-reruns.
- **Schema version** (`PRAGMA user_version`) — the 1.0 contract's "refuse
  and print the upgrade step" is impossible without it.
- **DSL-shaping features:** `Dir` token ([dir_outputs.md](designs/dir_outputs.md)),
  output validation (`Ensure`) + opt-in checksum *capture*, fail fast on
  missing external inputs.
- **One configuration design:** named profiles (a shipped `jasmin` profile)
  *and* the user/project config-file cascade, designed together — the config
  file format becomes a frozen surface, so design it once. Record the
  effective config per run.
- **Cheap, high-value tooling:** static DAG export (`remake dag`), `run-all`
  (only if it stays tiny).
- Done already: per-task resource capture
  ([resource_capture.md](designs/resource_capture.md)).

### 0.10.x — provenance capture + surface freeze preparation
- **Provenance capture:** environment (lockfile/env hash) and pipeline git
  hash/status per run — cheap, and export formats later depend on the
  history existing.
- **`remake verify`** — output reconciliation and adoption (the safe (a)+(c)
  core in [discussion.md](discussion.md)); mtime mode stays behind a flag.
- **Minimal run history** (`stats`) — per-run records; no dashboard.
- **Scale work driven by field data** — the file-side stat frontier on
  Lustre/NFS, `bench_field_scale.py` load-bearing in CI.
- **Settle the frozen surfaces:** the output-token interface (stat/size,
  batch listing, identity normalisation — so storage backends can be added
  later without breaking it); `--json` shapes with a version field; exit
  codes (0/1/2 documented and meaningful); `Remake` method naming aligned
  with the CLI and library use anchored to the remakefile dir; the declared
  public API in `docs/api/`; the location of `.remake/` (decide the
  "next to artefacts" question).
- **Design check only:** confirm `temp()`/scratch lifecycle can be added
  post-1.0 without a breaking schema change.

### 1.0 — the freeze; the contract begins
Ships once the remakefile DSL, public API and on-disk format are judged
stable. From here, [compatibility.md](compatibility.md) is binding: SemVer,
deprecation ramps, automatic on-disk migrations.

## After 1.0 — additive, in rough priority order

None of these gates 1.0; each is a new command, export or client over
surfaces frozen at 1.0.

1. **`temp()` / scratch intermediates** — high value for the niche; build
   when a real pipeline needs it (design checked in 0.10).
2. **Single-file HTML run report** (`remake report`) — the showpiece view
   over resource capture + DAG export.
3. **RO-Crate export** ([rocrate_export.md](designs/rocrate_export.md)) — a
   serialiser over the DB; rich because 0.10 capture has been recording.
4. **Storage-backend implementations** (S3/GCS/HTTP as declared
   dependencies) on the token interface settled in 0.10.
5. **Query by status**, output enumeration (`ls-tasks --paths`), clean
   verbs, richer dry-run — CLI additions as demand shows.
6. **Interactive web control plane** — *exploration only*, as a separate
   optional package/extra over the `Remake` API; gated on resolving the
   "live server vs detached SLURM batch tool" tension and login-node auth.
7. **Plugin entry points** — only if a third-party ecosystem appears;
   dotted-path injection covers the need meanwhile.

## Cross-cutting (continuous, not a milestone)

Scale-debt from [todos.md](todos.md) keeps pace with feature work,
calibrated against the design scale (~1e4 tasks × ~1e2 files/task = 1e6
*files*; see design.md "Scale target"): the file-side stat/
resolution frontier, quadratic diagnostics (`why`, `info --reasons`, `lint`
— review M20), bounding `retry_lock_commit`, zarr v3 `is_complete()`, and the
long-promised Hypothesis property tests (task keys, matrix normalisation).

## Explicitly *not* doing

- **Orchestrator daemon** (rejected as load-bearing; see discussion.md).
- **Passive read-only dashboard** (query the DB).
- **Dask-native integration** beyond the existing executor (dask misbehaves
  on JASMIN, remake's target).
- **Local resource budgets / task weights** — SLURM already handles
  resources and multiproc is not the primary target; stays on the menu only.
- **Intra-rule task dependencies** — would break the rule-level DAG that
  planning memory, array eligibility and failure-skip rely on.

---

## Assessment of the previous plan (2026-09-24)

The reasoning behind the re-plan, kept for the record. Test applied: does the
item change a frozen surface (DSL, public API incl. `--json`, on-disk
format)? If yes it must be designed before 1.0; if purely additive it can
follow.

**Previous 0.9.0**

| Item | Merit | v1? |
|---|---|---|
| Resource capture | Done; cheap, useful | — |
| Output validation (`Ensure`) + checksum capture | Medium: catches truncated outputs; early capture only pays if something reads it later | **Design yes** (DSL + schema); keep it small |
| DAG export | High value, near-free | No, but cheap |
| HTML run report | Nice showpiece; real maintenance cost | **No** — additive, post-1.0 |
| `jasmin` profile | Real migration value | **Yes, merged** with the config cascade — design the config format once |
| `Dir` token | High: many scientific tools write directories | **Yes** — DSL surface |
| `run-all` | Low: a shell loop does it | No; harmless if tiny |
| Fail fast on missing inputs | High, cheap | Yes (UX correctness) |

**Previous 0.10.x**

| Item | Merit | v1? |
|---|---|---|
| Env + git capture | High for "reliably recreate", cheap | Yes (additive schema, but history must start early) |
| `verify` / reconcile | High: scratch-purge recovery, adopting existing trees | Nice-to-have, additive |
| RO-Crate export | Niche; valuable for publication/citation | **No** — a serialiser, ideal post-1.0 |
| Stats store | Moderate; overlaps the report | Minimal only |
| Query by status | Useful, needs plan-time filtering | No — additive |
| Scale debt | Essential for the niche | Driven by field data |

**Previous 0.11.x**

| Item | Merit | v1? |
|---|---|---|
| Storage-backend tokens | Some relevance (JASMIN object store); big design | **Split:** settle the token *interface* pre-1.0 (it gets frozen, and it isn't settled — `Dir`, checksums, path-map dependencies); implementations post-1.0 |
| Plugin entry points | Low: no third-party ecosystem; dotted paths work; publishing ABCs multiplies the frozen surface | **Overkill** — post-1.0 if ever |
| Config cascade | Needed | **Yes** — folded into the 0.9 config design |

**Previous 0.12.x**

| Item | Merit | v1? |
|---|---|---|
| Interactive web control plane | Differentiator, but the most expensive item; contradicts "no server"; clashes with detached SLURM; login-node auth | **Overkill** — additive by design (another API client); separate extra, post-1.0 |
| `temp()` / scratch lifecycle | High for the niche (disk pressure); deep semantics | Design-check pre-1.0; build when needed |
| Local resource budgets / task weights | Low: SLURM handles resources | **Overkill** — dropped to the menu |

**Missing from the previous plan, but needed for 1.0** (from the review):
a schema version; fixing stored hashes and task keys before freezing them
(H2, H4, H5, M19); settled `--json` shapes and exit codes; a declared public
API with library use anchored to the remakefile dir (M13); and the
correctness bugs, above all H1, H3 and H6–H8.
