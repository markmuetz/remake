# Migrating remake2 remakefiles to remake3

Translate by reading and rewriting — there is deliberately no automated
tool. Read the whole remake2 file first; the long tail (helper methods,
implicit globals, class inheritance tricks) needs judgement, not regex.

## Recognising the source dialect

Two remake2-era styles exist:

```python
# Style A (remake ~0.7/0.8 era)
class Rule1(Rule):
    rule_matrix = {'a': [...], 'b': [...]}
    rule_inputs = {'in': 'data/{a}/{b}.in'}      # or a function
    rule_outputs = {'out': 'data/{a}/{b}.out'}
    def rule_run(inputs, outputs, a, b):          # note: no self
        ...

# Style B (older TaskRule era)
class Basic1(TaskRule):
    rule_inputs = {'in': 'data/in1.txt'}
    rule_outputs = {'out': 'data/out1.txt'}
    var_matrix = {'a': [...]}                     # name varies
    def rule_run(self):                           # self-based
        self.inputs, self.outputs, self.a ...
```

Both also relied on a module-level `rmk = Remake()` (or `ex1 = Remake()`)
with rules registered implicitly at class-definition time.

## The translation, item by item

| remake2 | remake3 |
|---|---|
| `class R(Rule/TaskRule):` | `@rule(...)` on a plain function |
| `rule_matrix` / `var_matrix` | `matrix=` (cartesian dict, tuple-key grouped dict, `list[dict]`, or callable — the remake2 tuple-key grouped form ports unchanged) |
| `rule_inputs` / `rule_outputs` class attrs | `inputs=` / `outputs=` decorator args (same dict/callable forms) |
| `def rule_run(inputs, outputs, a, b)` | the decorated function: `def fn([inputs,] [outputs,] <matrix keys>)` — but **omit `inputs` (and/or `outputs`) when there's no `inputs=`/`outputs=`**; see note below |
| `def rule_run(self)` + `self.inputs`/`self.a` | rewrite to explicit params: `def fn(inputs, outputs, a)` |
| implicit registration at class definition | `rmk.rules_from_current_module()` as the LAST line |
| dependency inferred from matching path strings | explicit `depends_on=[upstream_rule]` on every consumer |
| `Rule2.rule_inputs = Rule1.rule_outputs` | `inputs=rule1.outputs` (decorated fn carries `.outputs`/`.matrix`) |
| free use of module globals in `rule_run` | declare via `uses={'NAME': NAME, ...}` (see below) |
| `self.inputs` values were `Path` objects | inputs are plain strings, outputs path-like tokens — `Path(...)` wrap if Path methods needed |
| atomic tmp-file output writes | gone — rules write outputs directly (parent dirs pre-created) |
| pyquerylist filtering of `rmk.tasks` | `-Q 'python expr over kwargs'` on the CLI |
| `remake run-tasks file --tasks <keys>` | `remake run-task file <key-prefix>` |
| per-task SLURM jobs, job-name keyed | per-rule array jobs; `config={'slurm': {...}}` per rule unchanged in spirit |
| `partition='short-serial'/'long-serial'` (JASMIN) | `partition='standard', qos='standard'` (SLURM 25.11 renamed them) |
| `remake archive` | gone |
| `check_outputs_exist` config | `check_outputs='never'/'fallback'/'always'` Remake arg |

## Signature contract (stricter than remake2)

remake3 validates the rule function signature at decoration time against
`def fn([inputs,] [outputs,] <matrix keys>)`, and this is **not** quite
"the same contract" as remake2:

- remake2's `rule_run(inputs, outputs, ...)` always took both leading
  params, even for a rule with `rule_inputs = {}`. remake3 keys the
  signature off what you pass: **no `inputs=` → no `inputs` param** (and
  likewise for `outputs`). A download rule with no inputs becomes
  `def fn(outputs, idx)`, not `def fn(inputs, outputs, idx)`. Leaving a
  stale `inputs` param raises `SignatureError: signature must start with
  (outputs, ...)`.
- An empty `inputs={}` / `outputs={}` is rejected (`... is ambiguous —
  omit the argument`) — drop the decorator arg entirely rather than passing
  an empty dict.
- The params after the leading ones must be exactly the matrix keys (for a
  non-callable matrix); a mismatch is a `SignatureError` too.

So the no-input download/standalone rules need the most care — it's the
one place the mechanical `(inputs, outputs, ...)` carry-over breaks.

## Scope: the step that needs real judgement

remake2 `rule_run` bodies freely referenced module globals (constants,
helper functions, other classes). remake3 rejects undeclared free
variables at import (`ScopeWarning`/`ScopeError`) because untracked
globals make rerun decisions wrong.

For each free name in a rule body decide:
- imported *module* (`import numpy as np`, `import x.y as z`) → exempt;
- name bound by `from x import Name` (a class or function, e.g.
  `from matplotlib.lines import Line2D`, `from mymod import MyClass`) → the
  checker flags it like any other global and wants it in `uses={...}`. It is
  *not* exempt just because it came from an import — only whole-module
  imports are. For a local class this is correct (changes should rerun); for
  a stdlib/third-party class it's harmless tracking, declare it to silence
  the `ScopeWarning`;
- constant or helper function → add to `uses={...}` (it then correctly
  participates in rerun hashing);
- mutable module state / config object → restructure: pass through the
  matrix, or freeze into `uses`.

`uses` tracking is one level deep — a helper calling another helper
needs both declared.

### What "one level deep" actually means

This bit when translating `wescon_radar_dev.py` (1885 lines, ~45 helper
functions across several rules), so it's worth being precise:

- **`check_scope` (the `ScopeWarning` at import time) only inspects the
  decorated function's own body *and any `def`s nested inside it*.** It
  does NOT recurse into the body of a separate module-level function that
  the rule merely calls (even if that function is itself listed in
  `uses=`). So if `uses={'helper': helper}` and `helper` internally
  references `CONST` and `other_helper`, the *rule* does not need
  `CONST`/`other_helper` in its own `uses=` — only `helper` does.
- **`uses_hash` (rerun tracking) is the one that's "one level deep".** If
  `helper` changes its own source, the rule reruns (because `helper`'s
  AST is hashed). But if `helper` calls `other_helper` and only
  `other_helper`'s body changes, the rule's `uses_hash` does NOT change —
  nothing reruns. In practice this is rarely a real problem (editing a
  shared helper almost always means editing its signature/callers too,
  which *does* touch the declared function's source), but it's worth
  knowing about for helpers that are pure dispatch/delegation.

Net effect: when a module-level helper is shared across multiple rules
(can't be nested into just one), give each consuming rule a `uses=` entry
for *that helper only* — don't try to enumerate its transitive
dependencies too.

## A class with ~20 interdependent `@staticmethod` helpers

remake2 sometimes packed a single rule's logic into a `Rule` subclass
with a `rule_run` plus a couple of dozen `@staticmethod` helpers that only
call each other (e.g. `CompareDeltaZCandidates` in `wescon_radar_dev.py`:
~380 lines, 21 helpers, all interdependent, none used anywhere else).

If nothing outside the class uses these helpers, don't recreate them as
module-level functions with a wall of `uses=` entries and per-helper
dependency bookkeeping. Instead **nest every helper as a closure inside
the decorated rule function**:

```python
@rule(inputs=..., outputs=..., uses={...only the genuinely external names...})
def compare_delta_z_candidates(inputs, outputs, case, bracket_idx1, bracket_idx2):
    def load_data(...): ...
    def find_all_beam_alignment(...): ...
    def calc_cross_correlation(...): ...
    # ... 18 more ...

    # main body, calling the closures above
    ds1, ds2 = load_data(...)
    ...
```

Why this works well:
- `check_scope` walks into nested `def`s, so the closures' references to
  each other and to the rule's own params need no declaration.
- The whole rule (helpers + body) is one AST blob for `uses_hash`
  purposes — editing any helper changes the rule's own source, which
  *always* triggers a rerun. The "one level deep" caveat above doesn't
  apply because there's no separate `uses=` entry to go stale.
- `CompareDeltaZCandidates.plot_radarnet` collided in name with an
  unrelated module-level `plot_radarnet` used by a different rule — when
  nesting, rename to disambiguate (`plot_radarnet_comparison`).

Only promote a helper to module level (with its own `uses=` entries in
each consumer) if it's genuinely shared across rules — see
`sliding_offset_to_slices`, `load_data` (renamed from
`MatchRHIsToStorms.load_data`), and `plot_full_corr_matrix` in
`wescon_radar_dev.py` for examples of helpers that *had* to stay
module-level because 2+ rules call them.

## Dataclasses and other non-function objects in `uses=`

Older remake3 builds crashed (`AttributeError` / `TypeError: ... is a
built-in class`) if `uses=` contained a class (e.g. a `@dataclass` used
as a typed "context" object passed between nested closures) or a callable
object with no `__code__` (e.g. `scipy.stats.chi2`). Both are fixed as of
this migration:
- the file loader registers the loaded module in `sys.modules`, so
  `inspect.getsource` works for classes defined in a remakefile;
- `function_source`'s bytecode fallback now falls back to `repr(fn)` for
  callables that have no `__code__` at all.

So it's fine to do `uses={'CrossCorrelationResult': CrossCorrelationResult,
'chi2': chi2, ...}`. If you hit one of the old tracebacks, your remake3
checkout predates this fix (`src/remake/loader/__init__.py`,
`src/remake/core/scope.py`).

## "Translate but don't register" for disabled remake2 rules

remake2 rules with `enabled = False` (kept for reference, never run) have
no remake3 equivalent for "defined but disabled" — `@rule` always makes a
runnable rule object. Translate them fully (so the logic isn't lost and
still gets `ScopeWarning` checking), but **don't pass them to
`rmk.add_rules([...])`**. Use the explicit list form instead of
`rmk.rules_from_current_module()` so the disabled rules are simply
omitted:

```python
def find_camra_kepler_match(...):   # translated, ScopeWarning-clean
    ...

def plot_camra_kepler_match(...):   # translated, ScopeWarning-clean
    ...

def regrid_camra_kepler_l1(...):
    ...

rmk.add_rules([
    regrid_camra_kepler_l1,
    # find_camra_kepler_match and plot_camra_kepler_match intentionally
    # excluded: disabled in the remake2 original.
    ...
])
```

## Dynamic/callable matrices that read upstream outputs

remake2 patterns where a rule's matrix is computed by reading a small
upstream output file (e.g. "which bracket-pairs are deltaZ candidates,
read from a previously-written `.hdf`") and silently produces zero rows
if that file doesn't exist yet — translate the callable-matrix function
straight across, guarding with `.exists()`:

```python
def compare_delta_z_matrix():
    rows = []
    for case in conf.CASES:
        brackets_path = find_candidate_delta_z_outputs(case)['brackets']
        if brackets_path.exists():
            brackets = pd.read_hdf(brackets_path, key='brackets')
            for i in range(1, len(brackets)):
                if brackets.iloc[i]['deltaZ_candidate']:
                    rows.append({'case': case, 'bracket_idx1': i - 1, 'bracket_idx2': i})
    return rows
```

This preserves the original "deferred until upstream has run" behaviour
without needing `MatrixNotReady` — `remake info` just reports 0 tasks for
this rule until `find_candidate_delta_z` has actually written the
`brackets` files.

## `Path(outputs[...])` wrapping — when it's needed

remake3 output values are path-like tokens with `__fspath__` (so
`open(outputs['x'])`, `xr.Dataset.to_netcdf(outputs['x'])`, etc. all work
directly), but they are **not** `Path` instances — `.parent`, `.stem`,
`.exists()`, `.touch()`, `/` (joining) etc. raise `AttributeError`. Any
remake2 code that did `outputs['x'].parent / 'foo.png'` or
`outputs['dummy'].touch()` needs `Path(outputs['x']).parent / 'foo.png'`
/ `Path(outputs['dummy']).touch()`. This shows up constantly in plotting
rules (figure directories derived from a "dummy" output path) and in
multi-file-output rules (`Path(outputs[f'gridded_data{i}'])`).

## Metadata: no DB migration

remake3 starts a fresh `.remake/` (new schema, new task keys). Existing
on-disk outputs are NOT lost work: the default `check_outputs='fallback'`
recognises complete outputs for tasks with no DB record, so the first
plan after migration should show ~0 tasks to run for an up-to-date
pipeline. That is also the migration acceptance test. Once it passes,
lock it in with `remake set-state <file> -Q True --success
--check-outputs` — this records the adopted outputs in the DB, so later
plans stop re-statting every output file.

## Workflow

1. Read the remake2 file end-to-end; list rules, globals each `rule_run`
   touches, and the implicit input/output chains.
2. Translate per the table; wire `depends_on` explicitly to match the
   remake2 DAG (dump it — see below — don't eyeball the path strings).
3. `remake run newfile.py -n` — must import cleanly (signature/scope
   errors surface here) and the plan must make sense: for a pipeline
   whose outputs exist, expect ~0 runnable; "everything runnable" means
   outputs aren't where the new paths point.
4. Run one small slice: `remake run newfile.py -Q '<one task's kwargs>'`.
5. Full run.

Keep the remake2 file until step 5 has been verified.

## Get the ground-truth DAG from remake2 (don't infer it by hand)

For a multi-rule file, the `depends_on` wiring is the easy thing to get
wrong. Rather than reconstruct the DAG from matching path strings, dump it
straight from remake2 and use that as the target. remake2 builds the
rule-level graph as `rmk.rule_dg` (a `networkx.DiGraph` of rule classes;
`rmk.task_dag` is the task-level one). Load the file with remake2's own
loader and read the graph — `finalize=False` skips metadata/DB setup so it
works read-only without a configured `.remake/`:

```python
# run under the env that has remake2 installed (e.g. the old conda env)
import networkx as nx
from remake.loader import load_remake          # remake2's loader

rmk = load_remake('old_remakefile.py', finalize=False)   # cwd-relative
g = rmk.rule_dg
for u, v in sorted((a.__name__, b.__name__) for a, b in g.edges):
    print(f'{u} -> {v}')
print('topo:', [n.__name__ for n in nx.topological_sort(g)])
```

The migrated remake3 DAG must reproduce these edges. Two gotchas this
surfaces that a by-hand read misses:
- **`enabled = False` rules are absent** from `rule_dg` (remake2's
  `load_rules` skips them) — so a class count vs. node count mismatch tells
  you exactly which rules get the "translate but don't register" treatment.
- **isolated nodes** (a rule with no edges) are real — standalone
  download/plot rules, not a wiring bug.

If `load_remake` blows up on import (a stale module-level path stat, a
missing optional dep), that's also useful: it's a dependency the migration
must drop or stub anyway.
