# Graduated discussion items

Items from [discussion.md](discussion.md) that have been **designed and
implemented** — kept verbatim for the record (design reasoning, decisions and
their revisions, field measurements). The live ideas list stays in
discussion.md; when an item ships, it moves here with its implementation
postscripts. Parked sub-questions noted inline under an item remain open even
though the item itself shipped. A final section records **settled design
decisions** that produced no code — kept so they aren't relitigated without
new evidence.

## Graduated with full design records

- **Display code changes in `uses` functions (via a storage rework).**
  Just as code changes can be displayed for rules, we want to display them for
  `uses` functions. The naive framing ("save the code for each `uses` function
  and diff current vs stored") turns out to be entangled with a storage-shape
  problem worth fixing at the same time.

  *What exists today.* `uses` **is** already tracked for change detection, but
  awkwardly. `scope.uses_hash` renders each `uses` entry as its AST-normalised
  source (`ast.dump(ast.parse(...))`; plain values by `repr`; sourceless
  callables fall back to a bytecode `sha1`) and joins it into one string, which
  is stored **inline as TEXT on every `task` row** (`task.uses_hash`, alongside
  the equivalent `task.io_hash`). So the name is a misnomer — it's not a digest,
  it's a serialised AST — and the value is duplicated across every task of a
  rule. At 1e6 tasks that verbose blob is stored a million times, feeding
  straight into the per-task write-cost problem the perf section already flags.
  The stored form is also *normalised AST*, not raw source, so it diffs for
  equality but not into anything human-readable without `ast.unparse`.

  Meanwhile `run`/`inputs`/`outputs` code is stored the right way already: a
  content-addressed `code` table (`code(id, code TEXT)`) holding **raw source**
  (`inspect.getsource` via `Rule.source`), referenced once per rule by FK
  (`rule.inputs_code_id`/`outputs_code_id`/`run_code_id`). That is exactly the
  shape `uses`/`io` should adopt.

  *Chosen design.* Route `uses` (and `io`) source through the existing `code`
  table at **rule granularity**, and demote the per-task column to a genuine
  hash:
    - `uses` is a *dict* of N helpers, so it cannot be a single FK column on
      `rule` the way `run`/`inputs`/`outputs` are (one blob each). It needs a
      join table with **one row per helper**:
      `rule_uses(rule_id, name, code_id, kind)`, name-sorted. Ten `uses`
      entries → ten rows, each with its own `code_id`. `io` stays a single blob
      and can reuse the existing single-FK pattern.
    - **Heterogeneity is the crux.** `uses` entries are not all sourceable, but
      `code.code` is just `TEXT`, so all three cases store uniformly and only
      the *display* layer distinguishes them (via `kind`):
        - normal function (`inspect.getsource` works) → raw source, `kind =
          'source'`, real per-helper diff;
        - plain value (int/path/config) → `repr(value)`, `kind = 'value'`,
          trivial diff;
        - sourceless callable (REPL/`exec`, C func) → `<bytecode:sha1…>` label,
          `kind = 'bytecode'`, **not** diffable — display shows "source
          unavailable".
      Dedup still holds (content-addressed `code`, shared across rules); the
      `kind` marker lets the diff view decide what to render without
      re-inspecting the live object.
    - **Raw vs normalised split.** `code` stores *raw* source (for readable
      diffs), but change detection compares *normalised* AST. These are
      different strings — the same split `run` code already lives with. So the
      per-task `uses_hash`/`io_hash` columns become an **actual short digest**:
      `sha1` of the name-sorted *normalised*-AST string. Change detection stays
      exact, per-task rows shrink from a verbose AST dump to a fixed-length
      hash (a direct 1e6-task write/size win), and `code` independently holds
      the raw source for display.
    - Note the naming resolution: in this design the per-task column really is a
      hash, so `uses_hash` becomes the *correct* name and stays; the raw source
      lives in `code`. (If we ever ship the inline-storage form instead, the
      honest rename would be `uses_ast`/`io_ast` — but this design avoids that.)

  *What this unlocks.* Three things fall out of the same change: (1) the
  human-readable `uses` diff this item asked for — diff raw source in `code`,
  current vs stored, per helper name; (2) source available for `remake
  rule-info` (see todos) for free; (3) smaller/faster per-task records. Reuses
  `code` interning, `_ensure_rule` provenance, and `parse_uses_hash`'s per-name
  split.

  *Cost / sharp edges.* Schema migration on existing `.remake/` DBs (there's an
  established `ALTER TABLE ADD COLUMN` migration hook to follow, plus a new
  `rule_uses` table; alpha 0.8.0a0 so acceptable). Sourceless-callable fallback
  (REPL/`exec`) still can't show a source diff — store the bytecode-digest label
  as today and display "source unavailable".

  *Measured in the wild (2026-07-02, Mark Muetzelfeldt).* Reached from the
  opposite direction — a `remake info` that felt slow (see
  [bugs/04](bugs/04_info_redundant_and_superlinear_status_queries.md)) — and the
  numbers make the "worth fixing now" case concrete. On the
  `wescon_radar_dev.py` pipeline's `.remake/remake.db`: **272 MB total for just
  3341 task + 74 code + 12 rule rows**, and it is *not* free-page bloat
  (freelist ~0%; `VACUUM` reclaims nothing). Per-table (`dbstat`): the `task`
  table is **271.75 MB (99.8%)**, and within it the `uses_hash` column alone
  averages **~79 KB/row (max 142 KB)** — i.e. the entire database is the
  serialised-AST `uses` blob. Per-rule `COUNT(DISTINCT uses_hash)` is **1** for
  every rule (2 for one mid-refactor rule), confirming the value is byte-identical
  across a rule's tasks — pure duplication:

  | rule | tasks | distinct | avg len | total |
  |---|---|---|---|---|
  | compare_delta_z_candidates | 1465 | 1 | 145,668 | 213.4 MB |
  | regrid_camra_kepler_l1 | 774 | 1 | 47,354 | 36.7 MB |
  | plot_regridded_camra_kepler_l1 | 774 | 1 | 8,948 | 6.9 MB |
  | match_rhis_to_storms | 216 | 1 | 19,997 | 4.3 MB |
  | *(all rules)* | 3341 | — | — | **262.9 MB** |

  So the "1e6 tasks" hypothetical is not needed to feel this — a rule with a
  large `uses=` (here `compare_delta_z_candidates`, ~14 helpers) stores a 145 KB
  AST dump 1465 times = 213 MB from **one rule at ~1.5k tasks**; at 1e6 tasks the
  same rule would be ~140 GB. (`io_hash` is the same inline-TEXT shape but small
  here: avg ~2.4 KB, max ~4 KB.) *Correction:* an earlier version of this note
  guessed the `uses_hash` blob was also the mechanism behind bug 04's superlinear
  `get_tasks_status`. It is **not** — the log analysis
  ([logs_analysis/README.md](logs_analysis/README.md) §1.1–1.2, verified against
  `sqlite3_backend.py:248`) shows the status query never selects `uses_hash`; it
  `LEFT JOIN`s `code` and hauls back the full `code.code` run-source per task
  (256× amplification) for the planner's `CodeComparer`. So `uses_hash` drives DB
  *size* (this 272 MB), and the `code.code` JOIN drives query *time* — two
  separate problems with the same *shape* of fix (store a digest, fetch source
  lazily on a real diff). The DB bloat still matters: a smaller file warms in the
  page cache faster, damping the >1000× run-to-run variance the log analysis
  measured for the identical 1465-task query.

  *Implemented 2026-07-02 (stage A), with one revision to the chosen design.*
  The per-task column became an **integer FK** (`task.uses_code_id`/
  `io_code_id` → `code`), not a sha1 digest: the whole joined normalised
  string is interned by find-or-insert (content-addressed, so equal content
  ⇒ equal id, and unchanged-ness is int equality against the id
  `_ensure_rule` interns once per rule per invocation). Smaller than a
  40-char digest, *and* the old rendering stays recoverable by id — a digest
  would have degraded `why`'s before→after uses messages, since the prior
  rendering is unrecoverable from a hash. MM's probe ("wouldn't storing a
  hash somewhere be faster?") settled the trade-off: once per-task state is
  an int and content comparisons happen once per rule, a hash adds nothing
  measurable — its only win is the intern lookup itself, once per rule
  against a ~100-row table (an indexed `code.code_hash` remains a cheap
  follow-up if that table ever grows large). Alongside: `get_tasks_status`
  returns ids only (the code JOIN is gone; the planner resolves the distinct
  few ids per rule via the new `get_codes` and does set-membership per task);
  `_upsert_task` no longer computes `uses_hash` per task; rule code inserts
  intern too, so an edit-and-revert maps back to the original row instead of
  mass-rerunning; and the in-place migration backfills FKs from the inline
  columns, drops them and VACUUMs (the 272 MB recovers on first contact).
  Still open (stage B): the `rule_uses` per-helper raw-source table for
  readable source diffs and `rule-info` — until then a one-helper edit
  interns a full new joined string rather than sharing the unchanged
  helpers, and diffs remain normalised-AST-derived.

  *Field-verified 2026-07-02.* Ran the migration against the wescon-tools
  `.remake/remake.db` (3341 tasks): **272 MB → 964 KB** (~275×), content
  of every FK checked against a pre-migration backup (0 mismatches), no
  data loss, no mass rerun (`info` still 0-to-run), and `remake -D info`
  wall dropped ~10.2 s → ~4.3 s with the 1465-task status query down
  ~110× (2.2 s → 0.02 s). Details in
  [bugs/04](bugs/04_info_redundant_and_superlinear_status_queries.md)
  (*Field verification*).

  *Stage B implemented 2026-07-02, with a second key revision.* The
  per-helper table is `uses_manifest(uses_code_id, name, code_id, kind)` —
  keyed by the **uses version** (the joined-string id tasks store), not by
  `rule_id` as sketched above. Rule-keyed rows would be overwritten to the
  current version at ensure time, destroying the old side of the diff before
  the planner ever compares; version-keyed manifests are write-once and
  immutable, so any task's stored `uses_code_id` resolves to the raw helper
  sources it actually ran with, however many edits later. One row per
  helper, raw rendering interned individually in `code`
  (`scope.raw_uses_parts`; kind = source/value/bytecode), giving the
  per-helper sharing this section wanted: editing one of N helpers stores
  one new code row, the other N-1 are shared by FK. `why` now renders a
  real unified diff of helper source, before→after for values, "source
  unavailable" for bytecode-tracked callables, and degrades gracefully
  (bare "(body)") for records predating the table — no backfill is
  possible, the old raw sources were never stored. The joined normalised
  string still exists per version (it *is* change detection: one int per
  task); the manifest is display-only and change detection never reads it.

## Graduated (designed and implemented; kept for the record)

- **Single `.remake/` per directory: rule provenance + duplicate-rule-name
  guard.** **Done 2026-06-22.** One `.remake/remake.db` is shared by everything
  run in a directory, and identity is namespaced only by rule name + kwargs
  (`Task.key = sha1('<rule>:<kwargs>')`, task.py; `rule` looked up by name,
  sqlite3_backend.py) — *no* remakefile component. This is deliberate: it lets a
  pipeline split across imported files compose over one shared state/output
  store. The cost is when two *different* co-located remakefiles define a rule
  under the same name: they silently clobber each other's `rule`/`task` rows
  (spurious code-change thrash), `.remake/jobs/<rule>.json`, and the per-task
  log / SLURM-output dirs — and a code edit is indistinguishable from a
  collision without provenance. Shipped: a `remakefile` column on the `rule`
  table (recording which file last defined each rule; forward-compat migration
  in `_add_missing_columns`), threaded via `ensure_rules(..., remakefile=)`. The
  guard warns in `_ensure_rule` when a same-named rule's code differs *and* was
  last written by a different known remakefile — distinguishing a collision from
  an ordinary same-file edit; identical-source shared rules never warn. Tests in
  test_metadata.py. Provenance is independently useful (inspection, future GC of
  orphaned records once a remakefile is deleted).
  - *Still open (parked):* surface the same check statically in `remake lint`
    (pre-run, scans sibling remakefiles); and a `Remake(name=…)` namespace as an
    opt-in escape hatch if real collisions warrant full isolation. Default stays
    a warning (not an error) — co-location is rare and sometimes intentional.

- **Dynamic matrices: defer on *stale* upstream, not just *absent* (Fix
  A+B).** **Done 2026-06-17.** A callable matrix expanded at plan time from
  an on-disk output that an upstream was about to overwrite ran the wrong
  task set; the local replan loop self-healed but the single-plan SLURM
  path did not. Shipped the `@deferrable` marker refinement: the exception
  was renamed `MatrixNotReady → Defer` (no longer a `RemakeError` — it's a
  control-flow signal), and `@deferrable` (rule.py) marks a matrix as one
  that derives its task list from upstream outputs. Raising `Defer` from an
  unmarked matrix is a `SignatureError` (resolve_matrix, dag.py); the
  planner defers a `@deferrable` rule when any `depends_on` upstream is
  rerunning this wave (`_upstream_rerunning`, planner.py), not only when an
  upstream output is absent — so ordinary product callables are never
  over-deferred. Tests in test_pipeline.py (defer-on-rerun + the
  non-deferrable contrast), test_dag.py, test_rule_signature.py.
  - *Still open (parked):* could `@deferrable` optionally name *which*
    upstream outputs it reads, so the planner defers precisely on those
    rather than on any `depends_on` rerun? Currently any upstream rerun
    defers the rule (safe, occasionally an extra wave).

- **SLURM job ids written to file** — `.remake/jobs/<rule>.jobids.json`
  sidecars; consumed by `slurm-status`, `task-info`, resubmission and
  already-queued detection.
- **Per-task logging under SLURM arrays** — per-task key-named log files;
  see design_docs/per_task_logging.md.
- **Task inspection/validation** — `remake lint` (near-miss input wiring,
  missing depends_on).

## Settled design decisions (no code change)

- **Rule syntax: the `@rule` decorator, not a `class Rule` — settled
  2026-07-03.** Challenged pre-0.8.0: the decorator is neat for simple cases
  but doesn't *visibly* group a rule's parts, and every callable spec
  (inputs/outputs/matrix function) must be a *named* module-level function,
  scattering single-use `agg_inputs`/`event_matrix`-style defs around the
  file. The counterproposal was a modernised class — not remake2's: each slot
  an explicit `@staticmethod` (honest Python, no missing-`self` lie), helpers
  still declared via `uses=`, and each slot hashed *per-part* by the existing
  AST-normalised machinery (each staticmethod is a plain function, so
  `function_source` applies unchanged — no whole-class-body hashing, no
  invisible helpers). That version genuinely fixes most of what was wrong
  with remake2's classes, and it gives every rule function a canonical home.

  *Why the decorator still wins:*
  - **The costs land asymmetrically.** Only *callable* specs need names, and
    they are the minority in real pipelines (wescon: 2 deferrable matrices in
    8 rules; mcs_prime is dominated by dict specs + `inputs=gen.outputs`
    chaining). The class taxes *every* rule with `class X(Rule):` +
    `@staticmethod` boilerplate and an indent level to relieve pain felt by
    the tail. Syntax should optimise the common case.
  - **Two dialects is the disqualifying outcome.** remake2 itself ended with
    two class styles, and the migration guide's first section is "Recognising
    the source dialect". Offering class *alongside* decorator recreates that
    in docs/examples/skill; *replacing* the decorator would discard a
    field-validated API (two production JASMIN migrations, 32 rules, outputs
    identical) days before tagging. Replace-or-reject, never both.
  - **The function is the unit remake reasons about** — change detection,
    scope analysis, decoration-time `SignatureError` checks, `why`'s diffs
    all operate on plain functions; the decorator keeps the user-visible unit
    and the engine's unit the same thing. Chaining (`inputs=gen.outputs`)
    already gives "rule as object" without inheritance temptation. Ecosystem
    direction agrees (Luigi's class-Tasks → boilerplate reputation; Airflow
    added `@task` on top of class operators; pytest/click/Flask).

  *The named-function ceremony*, the strongest remaining objection, was
  probed separately via lambda source recovery (AST node location +
  `ast.unparse`) — proven feasible but **parked**: inline dicts and
  comprehensions already put most specs inside the rule block, and
  statement-shaped specs (e.g. `Defer`-raising matrices) can never be
  lambdas anyway. Full record under "Lambda source recovery" in
  [discussion.md](discussion.md).

  *Reopen bar:* a post-0.8.0 side-by-side prototype (the class front-end is
  ~50 lines compiling to the same `Rule` dataclass) rewritten against the
  *worst* real file (wescon's compare/gather chain), judged across the whole
  rule distribution — the class form must read better on the common case
  too, not just the callable-heavy tail, and would *replace* the decorator
  if adopted. Mitigation meanwhile: house style of a stacked, aligned
  decorator block; `<rule>_inputs`-style names placed directly above their
  rule; `remake rule-info` as the assembled one-rule view.
