# 0.11 soak: field notes

Field notes from the two-week JASMIN soak of the `release/0.11.0` branch
(design_docs/releases/v0.11.0.md, item 6). One entry per surprise: what
happened, the cause, and what was done. Newest last.

Soak pipelines so far: `wescon-tools` (`wescon_radar_dev.py`, v17).

## 1. Hashing a `uses` value that refers to itself never finished (fixed)

2026-11-23, `wescon_radar_dev.py`, branch at bc500b6.

- **Symptom.** `remake info wescon_radar_dev.py` ran >12 min at ~90% CPU and
  10 GB RSS with no output. The time went on `Remake.finalize` → `ensure_rules`
  → `rule_uses_hash` → `canonical_value`.
- **Cause.** Two rules had `uses={'chi2': scipy.stats.chi2}`. Its repr holds an
  address, so it is rendered through its `__dict__`, which holds several bound
  methods of itself and `np.vectorize` wrappers that lead back to it. With no
  cycle detection, only `_MAX_DEPTH = 40`, the rendering branched at every
  level: ~8x per +4 depth. The code is the same in 0.9 and 0.10; the July
  (0.8) remake did not render instances through `__dict__`.
- **Fix.** 1d6e4e6: `canonical_value` keeps the ids on the current rendering
  path and renders an object met again as `<cycle ^N T>`. Non-cyclic values
  hash as before (golden hashes; upgrade check vs 0.10.0, 21 ok). Fable review
  clean.
- **Result on JASMIN** (host839, load ~118): chi2 renders in 0.0003 s (7.3 kB,
  7 cycle markers). `remake info` takes 25–40 s wall at ~250 MB max RSS, of
  which ~5 s is CPU; the rest is NFS I/O (open_code 5.8 s, scandir 2.9 s,
  stat 2.1 s, sqlite 2.0 s).
- **Left open.** Shared, acyclic sub-objects are still rendered once per path,
  so heavy sharing is still exponential (predates the fix). Memoising each
  object's rendering within one hash would fix it.

## 2. `info` shows `?` for rules that are deferred only by an upstream rule (fixed)

2026-11-23, `wescon_radar_dev.py` v17 (nothing run yet). Requested by MM.

- **Symptom.** `remake rule-dag -N -M` shows `compare_rhis_to_radarnet[17](case)`
  and `match_rhis_to_storms[204](case, ...)`, but `remake info` shows both as
  `? ? ? ? ? deferred`. TOTAL says 1561 tasks, while rule-dag lists >1800 known
  tasks plus the deferred rules, so the two commands disagree without saying
  why.
- **Cause.** `Remake.info` (core/remake.py, ~750) treats every rule in `plan()`'s
  deferred set alike. That set mixes two cases:
  1. the rule's own `@deferrable` matrix raised `Defer`
     (`gather_delta_z_stats`, `compare_delta_z_candidates`): the tasks are
     unknown, so `?` is right;
  2. the rule has a static matrix and is deferred only because a rule upstream
     is (`compare_rhis_to_radarnet`, `match_rhis_to_storms`, `analyse_*`,
     `display_hdf_schemas`): its tasks are known, as rule-dag shows.
- **Proposal (case 2).** Real task count; failed and pending from the DB;
  up-to-date vs stale needs the plan, so show them combined or as `?`; to run =
  `deferred (<upstream rule>)`; included in TOTAL. `info --json` gains fields
  for the same.
- **Done.** `info` shows the task count, failed and pending (DB status) for a
  deferred rule with a static matrix, `to run` = `deferred (<root rules>)`,
  and a line under TOTAL counting deferred tasks. Every deferred `--json` row
  has `deferred_by` (`[]` when its own matrix is the cause); `totals` gains
  `deferred_tasks`. A callable matrix (deferrable or not) below a deferred
  rule stays `?`: the Fable review found that calling a plain one could crash
  `info` or give a wrong count.
- **Related, lower priority.** `gather_delta_z_stats` has a static matrix (one
  task per case) but raises `Defer` from it only as a gate, because its
  *inputs* callable reads the brackets file to list the files to gather. Only
  matrices can defer, so rule-dag shows `[?](?)` where `[17](case)` is true.
  Options: let an inputs callable be `@deferrable`, or let a deferrable matrix
  declare its keys or a static task list up front
  (`@deferrable(keys=['case'])`).
- **Also seen** (wescon side, not remake): a `ScopeWarning` in
  `analyse_compare_rhis_to_radarnet` for undeclared `CORR_PLOT_KIND` and
  `plot_corr_grid`. The wescon session is handling it.

## 3. `lint` reports false near misses for outputs of deferred rules (fixed)

2026-11-23, `wescon_radar_dev.py` v17 (nothing run yet). Reported by MM.

- **Symptom.** `remake lint` says `gather_delta_z_stats: matrix not ready,
  outputs unknown — skipped`, then reports inputs that rule produces as
  unproduced: a `NEAR MISS` pointing at an unrelated sibling file
  (`compare_rhis_to_radarnet.hdf`) and an inflated `external` count (68 inputs,
  e.g. `comparison/gathered_dZ_stats.hdf`).
- **Cost.** The false positive hides a real missing dependency: both
  `analyse_*` rules read `gather_delta_z_stats`' outputs without depending on
  it. That only shows once the deferred matrix can expand. The other
  `MISSING DEPENDENCY` findings in the same run were right (fixed wescon-side).
- **Proposal.** When any rule was skipped as deferred, don't call an unmatched
  input external or a near miss without saying so. Better: match unmatched
  inputs against the skipped rules' output *templates* (the format strings
  are known even when the matrix is not), which would also catch the missing
  dependency. At least, a summary line: N inputs not checked because rules X,
  Y were deferred.
- **Done.** Unmatched inputs are matched against deferred rules' output
  templates (`{field}` = text within one path component, the fixed leading directory canonicalised),
  so the wescon case becomes `MISSING DEPENDENCY analyse_* -> gather_delta_z_stats`.
  A deferred rule with callable outputs is named on near-miss and external
  findings (`unchecked`). Rules downstream of a deferred rule are still
  skipped: their inputs may come from callables that read missing files.
- **Recheck on wescon (bbc1ccb): didn't fire.** There, deferred rules'
  outputs are callables (`gather_delta_z_stats_outputs(case)`), so there was
  no template; and the "may be produced by deferred rules" caveat went on
  every external line, including thousands of raw inputs those rules can't
  make. Now: a callable outputs spec is called with a placeholder for each
  parameter it names, and the placeholders become one-component patterns
  (a call that raises leaves the rule unchecked); the caveat is one footer
  line. `unchecked` stays on the `--json` rows.
- **Second recheck (ec88c5b): works** on wescon c8f67d6:
  `gathered_dZ_stats.hdf` is MISSING DEPENDENCY → `gather_delta_z_stats`,
  no NEAR MISS, external down from 68 to the genuinely external radarnet
  files (`conf.deltaZ_outdir(case)` took the placeholder). But after the
  wescon depends_on fix, the `analyse_*` rules were "upstream matrix not
  ready — skipped": fixing the dependency made lint check less. Now a rule
  held only by a deferred upstream is checked like any other (its matrix
  expanded; inputs matched against the deferred rule's templates). A rule
  whose tasks or inputs raise while expanding (an inputs callable reading a
  file the deferred rule hasn't written) is skipped with a warning naming
  the error; before, that crashed lint.

## 4. `lint` NEAR MISS paths are hard to compare by eye (done)

2026-11-23, `wescon_radar_dev.py`. Suggested by MM.

- **Problem.** A NEAR MISS prints both full paths inline on one wrapped line.
  JASMIN paths are ~150 characters and share a long prefix, so the difference
  (here an extra `comparison/` directory and a different file name) is hard to
  spot.
- **Proposal.**
  1. Stack the two paths, labelled and aligned (`wants:` / `nearest:`), with the
     producing rule and its task count on the heading line.
  2. Diff by path component (`difflib.SequenceMatcher` on the `/`-split lists),
     so an inserted directory is one insertion and does not misalign the rest;
     a character diff only inside a changed component (`v16` vs `v17`).
  3. Collapse the shared prefix (and suffix) to `…/`, keeping the last one or
     two shared components for context.
  4. Colour through `Painter(args.colour)`: the differing span red in `wants`,
     green in `nearest`, shared parts dimmed; `^^^` markers under the
     difference when colour is off. `--json` stays plain, perhaps with a
     `diff` field.
- **Reuse.** The same helper suits anywhere two paths are shown together:
  `why`, the paths-changed reason, "expected X, found Y" messages.
- **Done** for `lint` (`remake/util/pathdiff.py`): stacked `wants:` /
  `nearest:`, component diff with a character diff inside similar
  components, prefix collapsed to `…` keeping one directory, colour or `^`.
  `--json` unchanged. Not yet used in `why` or elsewhere.

## 5. `slurm-status` output is hard to read (open; to item 8)

2026-11-23, `wescon_radar_dev.py` with a live v17 submission, bbc1ccb.
Reported by MM.

- **Symptom.**
  ```
  build_gridded_rhi_scans job 6058085      PD:454 R:306  [QOSMaxMemoryPerUser]
  plot_regridded_camra_kepler_l1 job 6058087      PD:763  [Dependency]
  compare_delta_z_candidates job 38926070     not in queue
  ```
  - No columns: the rule-name width varies, so job ids, counts and reasons
    start at a different offset on every line. No header or totals.
  - States are packed into one field (`PD:454 R:306`), so a column of
    running or pending counts can't be scanned. 454 + 306 = 760 of 763: the
    other 3 (completed? failed?) aren't shown.
  - "not in queue" doesn't say what it means. Here those are July job ids
    (389xxxxx) from the last submission of rules that are deferred now:
    finished, failed, cancelled, or just old?
  - The pending reason, the most useful field, is in brackets at the end.
- **Proposal.**
  ```
  rule                            job       tasks  PD   R    CD  F  reason
  build_gridded_rhi_scans         6058085   763    454  306  3   0  QOSMaxMemoryPerUser
  plot_regridded_camra_kepler_l1  6058087   763    763  0    0   0  Dependency
  compare_delta_z_candidates      38926070  -      -    -    -   -  not queued (last submitted 2026-07-10)
  TOTAL                                     ...
  ```
  For a job not in the queue, say when it was submitted and what the DB
  records (e.g. "DB: 17 succeeded"). Use `info`'s table rendering and
  `Painter`; `--json` unchanged. To be done in release item 8 (output
  readability), with its shared table helper.

## 6. Recheck notes on findings 2–4 (2026-11-23, bbc1ccb)

- **`info` names the root, not the nearest deferred rule.** Every held row
  says `deferred (compare_delta_z_candidates)`, including
  `gather_delta_z_stats`, whose own matrix would also defer. That is right
  for what the planner does (it never tries a rule's matrix while an upstream
  is deferred, so `compare_delta_z_candidates` is the only rule actually
  holding things up), but it surprised the reader. Open: show the nearest
  too, e.g. `deferred (gather_delta_z_stats ← compare_delta_z_candidates)`;
  to settle with item 8's vocabulary.
- **`lint` MISSING DEPENDENCY lines** are still ~260 characters with full
  paths: give them the stacked, `…/`-shortened layout (item 8 e).
- **`--colour` only before the subcommand** (`remake info --colour never`
  fails): item 8 d.
- **`lint` creates `.remake/remake.db`** in a fresh checkout ("Creating
  sqlite3 database"). A read-only check shouldn't create metadata: open it
  read-only, or skip the DB when there is none. Open.
