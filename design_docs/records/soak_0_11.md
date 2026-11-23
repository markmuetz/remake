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

## 2. `info` shows `?` for rules that are deferred only by an upstream rule (open)

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
