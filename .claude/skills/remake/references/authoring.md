# Writing remake3 rules

A remakefile is a plain Python module. Canonical shape:

```python
from pathlib import Path
from remake import Remake, rule

rmk = Remake(config={'slurm': {'partition': 'standard', 'qos': 'standard'}})

YEARS = list(range(1980, 2021))

@rule(outputs={'raw': 'data/raw/{year}.nc'}, matrix={'year': YEARS})
def extract(outputs, year):
    ...

@rule(inputs=extract.outputs, outputs={'clean': 'data/clean/{year}.nc'},
      matrix=extract.matrix, depends_on=[extract])
def clean(inputs, outputs, year):
    ...

rmk.rules_from_current_module()   # registration — LAST line, mandatory
```

`Remake(...)` at the top, `rmk.rules_from_current_module()` at the end.
Without the final call no rules are registered and the pipeline is empty.
Multi-file pipelines: define rules in any module (the decorator is
module-level), register with `rmk.rules_from_modules(mod1, mod2)`.

## The signature contract (checked at import)

```python
def fn([inputs,] [outputs,] <matrix keys...>):
```

- `inputs` param iff `inputs=` declared; `outputs` param iff `outputs=`
  declared; both optional (side-effect rules may have neither).
- Remaining params must be exactly the matrix keys (any order). No
  `*args`/`**kwargs`.
- Violations raise `SignatureError` at import — by design (fail at
  import, not 3 hours into a SLURM job).

## inputs / outputs

- Dict form: `{'name': 'path/with/{matrix_key}.nc'}` — values are format
  strings over matrix keys. Empty dicts are rejected; omit instead.
- Callable form (fan-in / non-uniform): a function of a *subset* of the
  matrix keys returning the dict:
  ```python
  def winter_inputs(year):
      return {m: f'data/{year}/{m}.nc' for m in ('dec', 'jan', 'feb')}
  @rule(inputs=winter_inputs, ...)
  ```
- `rule_b.inputs = rule_a.outputs` is the chaining idiom (plus
  `depends_on=[rule_a]` — wiring is NOT inferred from paths).
- Output parent directories are created by remake before the task runs —
  never `mkdir` in rule code.
- Outputs are wrapped in tokens: strings become `FileToken`
  (transparent: pass straight to `open()`/xarray). Use `ZarrStore(path)`
  / `S3Object(...)` explicitly, or subclass `OutputToken` for custom
  completion semantics (e.g. a DB row).

## matrix

Three forms:
- `{'year': YEARS, 'model': MODELS}` — cartesian product;
- `[{'year': 1980, 'cluster_id': 'c1'}, ...]` — explicit task list;
- a zero-arg callable returning `list[dict]` — a *dynamic* matrix.
  Raise `MatrixNotReady(path)` while upstream outputs are missing; the
  planner defers the rule and retries after each wave (locally via the
  replanning loop, on SLURM via a continuation job).

Matrix values become task kwargs and must be JSON-serialisable (they
ride through SLURM job specs) and stable in repr (they define the task
key — don't use objects whose repr changes between runs).

## Scope and uses

Rule functions may not reference free module globals — remake can't
track them for reruns. At decoration:
- stdlib modules/objects and imported modules are exempt (fine to use);
- anything else undeclared → `ScopeWarning` (or `ScopeError` with
  `strict_scope=True` on the rule or the Remake).

Declare data and helpers via `uses=`:
```python
@rule(..., uses={'THRESH': THRESH, 'detrend': detrend})
def analyse(inputs, outputs, year):
    x = detrend(...)        # injected at execution
```
`uses` participates in rerun decisions: changing a value's repr or a
function's code (AST-level — formatting doesn't count) reruns the rule's
tasks. Classes work too (the whole class body is hashed). Tracking is
one level deep: a `uses` function calling *another* module-level helper
won't see changes to that helper — declare it too. Same for classes:
inherited methods live in the base class, so declare the base in `uses`
as well if its changes should trigger reruns.

## SLURM config

```python
@rule(..., config={'slurm': {'mem': '16G', 'time': '2:00:00'}})
```
merged over `Remake(config={'slurm': {...}})` defaults
(partition=standard, qos=standard, time=4:00:00, mem=4G; `account` is
site-specific — set it in the Remake config). Any key becomes
`#SBATCH --key=value`. Special keys: `array_threshold` (min tasks for an
array job, default 10), `array_throttle` (`--array=0-N%T`).

## Checking your work

```
remake lint myfile.py          # input/output wiring between rules
remake run myfile.py -n        # plan only: task counts, deferrals
remake info myfile.py [-t]     # status table
remake run myfile.py -Q '...'  # run a small slice first
```
Common authoring errors surface at import (signature, scope, empty
dicts) — `remake info` failing to load IS the error report. `remake
lint` (exit 1 on findings) catches the next layer: NEAR MISS = an input
no rule produces but an upstream produces something almost identical
(format-string typo, off-by-one kwarg); MISSING DEPENDENCY = an input
another rule produces without a `depends_on` declaring it (ordering
bug). `external` rows are informational — source data is expected.

## Known limitation

No task-level DAG: dependencies are rule-to-rule. Two tasks of the same
rule cannot depend on each other (e.g. timestep N+1 needs timestep N) —
restructure as separate rules or a single sequential task.
