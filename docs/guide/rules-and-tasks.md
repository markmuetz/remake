# Rules and tasks

A **rule** is a decorated function describing how to turn inputs into outputs.
A **task** is one concrete instance of a rule — after matrix expansion, a
single rule can produce thousands of tasks.

## Anatomy of a rule

```python
@rule(
    inputs     = {'raw': 'data/raw/{site}/{year}.csv'},
    outputs    = {'clean': 'data/clean/{site}/{year}.csv'},
    matrix     = {'site': SITES, 'year': YEARS},
    depends_on = [upstream_rule],
)
def clean(inputs, outputs, site, year):
    ...
```

- **`inputs` / `outputs`** — dicts of named paths. `{placeholders}` are filled
  per task from the matrix.
- **`matrix`** — the grid of tasks. `{'site': SITES, 'year': YEARS}` is the
  Cartesian product; a `list[dict]` gives explicit (non-Cartesian)
  combinations.
- **`depends_on`** — upstream rules, establishing DAG edges.
- The function signature takes `inputs`, `outputs`, and one argument per matrix
  dimension.

## Chaining rules

Reuse an upstream rule's `outputs` (and often its `matrix`) directly:

```python
@rule(
    inputs     = extract.outputs,
    outputs    = {'processed': 'data/processed/{site}/{year}.nc'},
    matrix     = extract.matrix,
    depends_on = [extract],
)
def process(inputs, outputs, site, year):
    ...
```

## Fan-in with callable inputs

When a task needs *many* upstream outputs (e.g. all years for a site), pass a
callable that returns the input dict for each task:

```python
def aggregate_inputs(site):
    return {str(year): f'data/processed/{site}/{year}.nc' for year in YEARS}


@rule(
    inputs     = aggregate_inputs,
    outputs    = {'agg': 'data/aggregated/{site}.nc'},
    matrix     = {'site': SITES},
    depends_on = [process],
)
def aggregate(inputs, outputs, site):
    ...
```

This is fan-in: one `aggregate` task per site, each consuming every year.

## Tracking code and constants with `uses`

remake hashes each rule's function body. If a rule depends on a module-level
constant or helper function, declare it with `uses` so changes to it also
trigger reruns:

```python
THRESHOLD = 0.5

@rule(inputs=..., outputs=..., uses=[THRESHOLD, helper_fn])
def filter_rows(inputs, outputs):
    ...
```

See `examples/ex3_uses_scope.py` for the full semantics (one level deep;
classes are supported — the whole class body is hashed).

## Registering rules

End the module with:

```python
rmk.rules_from_current_module()
```

This collects every `@rule` defined in the module onto the `Remake` object.
For pipelines split across modules, see `examples/ex7_multifile/`.
