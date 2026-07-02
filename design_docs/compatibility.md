# Backwards-compatibility policy

How much breakage remake permits itself, and when. Decided 2026-06-23.
See [roadmap.md](roadmap.md) for how the freeze milestones below are
sequenced.

## The headline

**Pre-1.0: breaking changes are permitted, with one carve-out (on-disk
state). Post-1.0: strict [SemVer](https://semver.org), with deprecation
ramps.** 0.x is for getting the surfaces right; 1.0 is the promise we then
keep.

## Three surfaces, three costs

The cost of a breaking change depends entirely on *what* breaks. remake has
three distinct compatibility surfaces, and they get different treatment even
before 1.0:

| Surface | What it covers | Pre-1.0 policy | Why |
| --- | --- | --- | --- |
| **Python / CLI API** | `Remake` methods + their return shapes, command names + flags, exit codes | Break freely; record every break in `CHANGELOG.md` | Cost to the user is *edits* — cheap to absorb, easy to grep/fix. |
| **Remakefile DSL** | `@rule` and its keywords (`inputs`/`outputs`/`matrix`/`depends_on`/`uses`/`config`/`strict_scope`/`name`), token types, registration helpers | Break freely **but stabilise earliest** — this is the first surface frozen approaching 1.0 | This is the user's *source code*. Churn means rewriting pipelines, the most expensive kind of edit. |
| **On-disk state** | `.remake/` — the SQLite schema (`remake.db`), job specs (`.remake/jobs/*.json`), jobid + per-task result sidecars, log layout | **Do not break gratuitously even now.** Ship a schema migration, or at minimum detect an old layout and fail with a clear "rebuild required" message — never silently misread it | Breakage forces a **full rebuild of an already-completed pipeline**. At remake's target scale (~1e4 tasks producing ~1e6 files, expensive SLURM jobs on JASMIN — see remake3_design.md "Scale target") that is hours-to-days of compute, falling on exactly the users remake exists for. |

The on-disk carve-out is not new policy bolted on — the SQLite backend already
ships a defensive `ALTER TABLE` migration path, so "the DB gets ramps even in
0.x" is the *existing* precedent, now stated explicitly.

## Pre-1.0 conduct

- Every API/CLI/DSL break goes in `CHANGELOG.md` under **Changed** (or
  **Removed**), with the migration in one line.
- On-disk schema changes ship with a migration or an explicit version check;
  a run against an incompatible `.remake/` must fail loudly, not corrupt or
  misread it.
- No formal deprecation cycle is owed pre-1.0, but prefer one when it is cheap
  (an alias, a warning) — especially for the remakefile DSL.

## The 1.0 contract

1.0 ships once the **remakefile DSL**, the **public Python/CLI API**, and the
**on-disk format** are judged stable (the freeze is the 0.12.x → 1.0 step in
the roadmap). After 1.0:

- **SemVer is binding.** Breaking changes to any of the three surfaces happen
  only in a **major** release.
- **Deprecation ramps.** A feature slated for removal is deprecated for at
  least one minor release first, emitting a warning that names the
  replacement, before it is removed in the next major.
- **On-disk migrations are forward-only and automatic** where possible; where
  not, remake refuses to run and prints the upgrade step.
- **The public surface is declared.** What `docs/api/` documents (the exported
  `remake` package symbols) is the supported API; everything else
  (underscore-prefixed members, `core.*` internals, the exact text of log
  lines) is explicitly *not* covered, so internal refactors never count as
  breaking. This is what lets the "CLI is a thin render layer over a complete
  Python API" principle (see `remake3_design.md`) stay refactorable.

## Out of scope for the guarantee (always)

Even after 1.0, these are never part of the compatibility promise: exact
log-line wording, the formatting of human-readable (non-`--json`) command
output, private/`core.*` internals, and timing/scheduling details of when
tasks run within a wave.
