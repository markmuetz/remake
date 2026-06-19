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
- **`matrix`** — the grid of tasks (see [Matrix forms](#matrix-forms) below).
- **`depends_on`** — upstream rules, establishing DAG edges.
- The function signature takes `inputs`, `outputs`, and one argument per matrix
  dimension.

## Matrix forms

The `matrix` can be written four ways:

```python
# 1. Cartesian product of independent dimensions.
matrix = {'site': SITES, 'year': YEARS}

# 2. Tuple key: bind several kwargs together — an explicit, pre-filtered
#    sequence of combos instead of the full product (mix with scalar keys
#    freely; each dict entry is one product axis).
matrix = {('site', 'year'): [('oxford', 2010), ('bristol', 2015)]}

# 3. Explicit list[dict]: one task per dict.
matrix = [{'site': 'oxford', 'year': 2010}, {'site': 'bristol', 'year': 2015}]

# 4. Callable returning list[dict]: a *dynamic* matrix, resolved at plan time.
#    Mark it @deferrable and raise Defer(path) while upstream outputs are
#    missing; remake defers the rule until they exist.
@deferrable
def matrix():
    ...
```

### Dynamic matrices

Sometimes the set of tasks isn't known until an upstream rule has run — e.g. a
detection step writes a variable number of events per year, and you want one
downstream task per event. Make `matrix` a zero-argument callable that reads
those upstream outputs and returns the `list[dict]`, and mark it `@deferrable`:

```python
from remake import Defer, deferrable

@deferrable
def event_matrix():
    rows = []
    for year in YEARS:
        path = Path(f'data/events/{year}.json')
        if not path.exists():
            raise Defer(path)                 # outputs not ready — defer
        rows += [{'year': year, 'event_id': e['id']}
                 for e in json.loads(path.read_text())]
    return rows
```

remake calls the matrix during planning. While the inputs it needs are missing
it raises `Defer`, and remake **defers** the rule — running its upstream first,
then retrying. A single `remake run` resolves the whole chain (locally via a
replanning loop; on SLURM via a continuation job). See
`examples/ex8_dynamic_matrix.py` for a complete dynamic matrix + dynamic fan-in.

The `@deferrable` marker is required to raise `Defer`: it makes the dynamic
contract explicit (raising `Defer` from an unmarked matrix is an error). It
also lets the planner defer the rule while its upstream is *rerunning* — not
only when the upstream output is *absent* — so the matrix never expands from a
stale, about-to-be-overwritten output. An ordinary callable matrix that merely
computes a product (no upstream reads) needs no marker and is never deferred.

Matrix values become task kwargs, so they must be JSON-serialisable and stable
in `repr` (they define the task identity).

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

## Fan-in

When a task needs *many* upstream outputs (e.g. all years for a site), build an
inputs dict with one entry per upstream output. The matrix key (`site`) stays a
`{...}` placeholder — escaped as `{{site}}` so the f-string leaves it for remake
to fill per task — while the fan-in dimension (`year`) is baked in:

```python
@rule(
    inputs     = {str(year): f'data/processed/{{site}}/{year}.nc' for year in YEARS},
    outputs    = {'agg': 'data/aggregated/{site}.nc'},
    matrix     = {'site': SITES},
    depends_on = [process],
)
def aggregate(inputs, outputs, site):
    ...
```

This is fan-in: one `aggregate` task per site, each consuming every year.

When the set of inputs can't be known at module load — it depends on the matrix
value, or on upstream outputs that don't exist yet — pass a *callable* of the
matrix keys instead, returning the dict per task. See
`examples/ex5_callable_inputs_matrix.py`.

The callable can be a named function or a `lambda`:

```python
@rule(
    inputs     = lambda site, year: {'in': f'data/{site}/{year}.nc'},
    outputs    = {'out': 'data/processed/{site}/{year}.nc'},
    matrix     = {'site': SITES, 'year': YEARS},
    depends_on = [extract],
)
def process(inputs, outputs, site, year):
    ...
```

Its parameter names must be matrix keys: remake calls it with the matching
kwargs (extra kwargs it doesn't name are dropped, so `lambda site:` is fine; a
parameter that *isn't* a matrix key raises `TypeError`). A `lambda` works, but
change detection is coarser for one than for a named `def` — remake hashes the
callable's source, and a lambda's source is captured as raw line text rather
than compared by code structure, so cosmetic edits on that line can trigger
reruns and two lambdas sharing a line are indistinguishable. Prefer a named
function for non-trivial wiring.

!!! warning "Closures are invisible to change detection"
    A callable that closes over an outer variable — `lambda site: {'in':
    BASE / f'{site}.nc'}` — hashes only its own source, *not* the captured
    value. Changing `BASE` will **not** trigger a rerun. This applies to
    named functions too, not just lambdas. Thread such values through the
    matrix, or declare them with [`uses`](#tracking-code-and-constants-with-uses),
    if you want them tracked.

## Tracking code and constants with `uses`

remake hashes each rule's function body. If a rule depends on a module-level
constant or helper function, declare it with `uses` so changes to it also
trigger reruns:

```python
THRESHOLD = 0.5

@rule(inputs=..., outputs=..., uses={'THRESHOLD': THRESHOLD, 'helper_fn': helper_fn})
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
