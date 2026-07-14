# Remakefile dependencies + discovery (`run-all`)

> **Status: PARKED (MM, 2026-07-14).** Design captured from discussion,
> not scheduled for a release. Parked because the cross-remakefile rerun
> story is messy: when a task in the upstream remakefile goes stale, what
> should rerun downstream has no clean answer (see §Staleness — the reason
> it's parked). Revive by settling that section first; the rest of the
> design is ready. One piece is independently worthwhile regardless —
> §Definition vs visibility — stubbed in [todos.md](todos.md).
> Class: **Design**.

## Motivation

Projects split naturally into pipelines: e.g. a *download* remakefile,
then a *process* remakefile (MM has several such). Wanted:

1. **Remakefile dependencies** — `process.py` declares it depends on
   `download.py`.
2. **Discovery / `run-all`** — in a directory, find all remakefiles and
   run them, respecting the declared dependencies.

## Design (as discussed)

### Remakefile dependencies

- Declaration: `Remake(requires=['download.py'])`, paths relative to the
  declaring remakefile (consistent with the post-0.8.0 anchor rule).
  `load_remake` resolves recursively; cycles detected with the same
  networkx check as the rule DAG.
- Semantics: **ordering + gate**, not task-level linkage. `remake run
  process.py` first loads and plans `download.py`; if the upstream has
  runnable tasks → error ("upstream remakefile not up to date — run it
  first, or use run-all"). No auto-run from plain `run` (could silently
  submit someone else's SLURM pipeline); `run-all` is the explicit
  opt-in.
- Co-located remakefiles already share one `.remake/` store, so no
  metadata work for the same-directory case.

### Discovery / `run-all`

- **Discovery must not import**: identify candidates with a cheap AST
  scan (a `Remake(...)` call / `@rule` decorators), then `load_remake`
  only those. Importing every `.py` executes side effects
  (`make_example_data.py` would run).
- `run-all`: discover in cwd (flag for recursive), topo-sort by
  `requires`, error on cycles, run each remakefile only after its
  upstreams report fully up to date. Subdirectory remakefiles run in
  their own `.remake/` stores (existing chdir behaviour).
  `examples/` is a ready-made integration test.
- **SLURM staging**: the SLURM executor returns after submission, so
  sequential run-all can't just proceed. v1 = sync executors only, clean
  error under SLURM. v2 sketch: the jobids sidecars record every
  submitted job, so the downstream remakefile's first-wave sbatch can
  carry `--dependency=afterok:` on all upstream jobs — coarse but
  correct, reusing the continuation machinery hardened 2026-07. Own
  design pass when revived.

## Staleness — the reason it's parked

Within one remakefile, "upstream reran → downstream reruns" works via
rule-level `depends_on` + run_seq stamps. Across remakefiles there are no
rule-level edges, and every option considered has a flaw:

- **No propagation (the v1 proposal):** if `download.py` re-fetches an
  existing file in place, `process.py` never learns. `@deferrable`
  matrices cover *new* upstream outputs (new file → new downstream task)
  but not changed-in-place ones. Honest but incomplete — and the
  incompleteness is silent.
- **Coarse propagation** (any upstream run that did work → downstream
  remakefile reruns everything): one new downloaded file mass-reruns the
  whole processing pipeline — exactly what remake exists to avoid.
  Rejected outright.
- **Rule-level cross-file `depends_on`** (import the other remakefile's
  rule objects): the reference mechanism falls out of the
  definition-vs-visibility fix below, but ownership is unsolved — whose
  plan contains the foreign rule's tasks, who runs them, which remakefile
  do they belong to in the DB? Deferred.

MM's verdict (2026-07-14): "messy when you start to think about what
needs to rerun when a task in the first remakefile becomes stale" — park
until a clean propagation story exists. A revival should probably start
from per-remakefile completion stamps in the shared `meta` table plus
*file-level* linkage (downstream tasks whose declared inputs are upstream
outputs), and check the cost against the scale target before committing.

## Definition vs visibility (worth doing regardless)

Cross-file references break the "scan the namespace" idioms in three
places, found while designing this (the trigger: two Remake objects
visible in one file):

1. `load_remake` collects every `Remake` in `vars(module)` — a natural
   `from download import rmk as download_rmk` in `process.py` trips
   "More than one Remake defined".
2. **Worse, and silent:** `rules_from_current_module()` would claim an
   *imported* rule (`from download import fetch` for
   `inputs=fetch.outputs`, the cross-file version of ex1's idiom) into
   the importing pipeline — duplicate registration, collision warnings
   against the shared store, the foreign task running under the wrong
   remakefile.
3. Deliberately two Remakes in one file: keep disallowed. The DB keys
   rules by remakefile path, the CLI addresses pipelines by file path,
   and SLURM scripts re-invoke `remake ... <remakefile>` — a `file.py:name`
   selector would need threading through all of it, and remakefile deps
   (when revived) make the split-into-two-files answer natural.

Fix for 1–2: record where objects are **defined**, not where they're
visible. `Remake.__init__` captures its defining file (caller frame
`__file__`); rule functions already know their module reliably
(`inspect.getsource` is load-bearing for code hashing). `load_remake`
filters to Remakes defined in the loaded file;
`rules_from_current_module` filters to rules defined in the current
module. Imported objects become inert references — usable, never
claimed. Independent of the parked feature; stubbed in todos.md.
