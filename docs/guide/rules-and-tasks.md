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

### Plumbing is checked up front

Wiring mistakes raise `SignatureError` once, early — at decoration for static
matrices, at first expansion for callable ones — rather than failing every
task identically at run time:

- the signature must start with `(inputs, outputs, ...)` matching the declared
  specs, and its remaining parameters must equal the matrix keys;
- an inputs/outputs *template* may only reference `{placeholders}` that the
  matrix provides (`'data/{stie}/{year}.csv'` against a `site` matrix is
  caught at import, naming the offending field);
- a callable `inputs`/`outputs` may only require parameters the matrix
  provides;
- `*args`/`**kwargs` and empty `inputs={}`/`outputs={}` are rejected.

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
`examples/ex10_dynamic_matrix.py` for a complete dynamic matrix + dynamic fan-in.

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

## Which upstream tasks a task depends on

`depends_on` links *rules*. Which upstream *tasks* a task needs comes from
its declared `inputs`: a task depends on the upstream tasks whose outputs
appear among its inputs. So when `extract[year=2001]` reruns (or fails), only
the tasks that read its outputs rerun (or are skipped) — `process[year=2001]`
above, a `process[year=2002]` that reads the previous year, or the one
`aggregate` task for that site — whatever the matrices look like.

Two cases fall back to "depends on every upstream task":

- **No path link** — the task's inputs include none of the upstream's outputs
  (`depends_on` used only for ordering). Any upstream rerun reruns it.
- **Shared outputs** — it reads an output that several upstream tasks write
  (e.g. one zarr store each task writes a region of).

The contract: **declare what you read.** A task that reads upstream files
it doesn't list in `inputs` (say, by globbing a directory) is invisible to
this, and won't rerun when they change. The `inputs=upstream.outputs` idiom
is recognised from the templates alone, so it costs nothing at plan time;
other shapes resolve the paths, once per plan and only when an upstream task
is actually rerunning or newer. That shortcut assumes matrix values are
plain names or numbers: keep `/`, `.` and `..` out of string values used in
paths.

When the set of inputs can't be known at module load — it depends on the matrix
value, or on upstream outputs that don't exist yet — pass a *callable* of the
matrix keys instead, returning the dict per task. See
`examples/ex4_callable_inputs.py`.

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
parameter that *isn't* a matrix key raises `TypeError`). A `lambda` works as
well as a named `def`: since 0.9.0 remake cuts the lambda out of the line it
sits on and compares its code structure, so editing the `matrix=` or
`outputs=` next to it is not an inputs change. (Two lambdas with the same
arguments on one line are compared together.) Prefer a named function for
non-trivial wiring anyway: it reads better in `remake why`.

A callable spec is also re-resolved at plan time and compared with the paths
the task last ran with, so a fan-in built from a module global —
`inputs=lambda: {y: f'in/{y}.nc' for y in YEARS}` — reruns when `YEARS`
shrinks or grows, even though its source didn't change.

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

### Undeclared names

remake checks each rule for names it reads from outside its own body that
aren't in `uses`, and warns (`ScopeWarning`) — or, with
`Remake(strict_scope=True)` or `@rule(strict_scope=True)`, raises
`ScopeError`. The check runs once the remakefile has finished executing, so
helpers and constants defined *below* a rule are checked too.

Library modules, and the classes, functions and objects they provide
(`np`, `Path`, `from loguru import logger`), are environment and never
reported. Your project's values are, even when they look like library
objects: `DATA_DIR = Path('/data')` or `PATTERN = re.compile(...)` defined in
the remakefile, a module of your own under the remakefile's directory, or a
global that happens to be called like a stdlib module (`site = 'CHIL'`).
Declare them in `uses`.

### What counts as a change

Since 0.9.0 each `uses` value is rendered canonically, so a rerun means
something really changed:

- **Functions and classes** by their code structure (comments and whitespace
  don't count; docstrings do). Functions without source (defined with `exec` or at
  a REPL) by their compiled code; builtins and numpy ufuncs by name
  (`np.log` vs `np.log10` is a change); `functools.partial` by the function and
  its arguments. Containers are rendered element by element, so
  `uses={'steps': [clean, regrid]}` tracks both functions' source.
- **Sets** with their elements sorted and **dicts** with their keys sorted:
  order is not a change (`==` ignores it), and a set of names no longer looks
  changed in every new process.
- **numpy arrays** by dtype, shape and a hash of their bytes (a `repr`
  truncates past 1000 elements and rounds floats); **pandas** objects by their
  values, index, column names and dtypes.
- **Modules** under the remakefile's directory (your own helper modules) by
  their source, e.g. `uses={'params': params}`. Installed libraries by name
  only: upgrading them is environment, not code.
- **Anything else** by its `repr`, if that is stable. An object whose `repr`
  holds a memory address (`<Foo object at 0x7f…>`) is rendered from its
  attributes instead; if it has none, only its type is recorded and changes
  to it go unseen.
- Your own types can say how they should be compared: give them a
  `__remake_hash__(self) -> str` method.

The `@rule(...)` decorator itself is not the rule's code: editing `matrix=`,
`config=` (an OOM `mem` bump) or `name=` doesn't rerun the tasks that already
ran. Decorators *below* `@rule` wrap the function and do count. Reordering
the keys of an `inputs`/`outputs` dict is not a change either (see
[input order](#input-order)), and neither is adding or changing an
[`Ensure`](#output-checks-with-ensure) check.

Upgrading remake or Python does not rerun anything: records written by
earlier versions are matched against the new renderings and re-stamped on the
next `remake run`. To check a pipeline is stable, run it, then
`remake run pipeline.py -n` should plan nothing; `remake why` names any
`uses` entry that differs.

!!! warning "A `uses` key that shadows a different module global warns"
    Inside the rule, the `uses=` value wins for that name. If a `uses` key
    matches a module global bound to a *different* value —
    `uses={'normalise': normalise_v1}` in a module that also defines
    `normalise` — the reader sees one definition while another runs, so
    remake warns (`ScopeWarning`). The same goes for a variable of an
    enclosing function — a rule factory's parameter: since 0.9.0 the `uses`
    value wins there too. The standard idiom
    `uses={'normalise': normalise}` (declaring the same object for tracking)
    stays silent. If the warning fires after an intentional edit, rename the
    uses key or update the global.

## Placeholders, input order and shared outputs

### Format specs in placeholders

A placeholder is filled with Python's `str.format`, so a format spec works.
With an integer matrix value, `{runid:03d}` zero-pads it:

```python
@rule(
    outputs = {'nc': 'atmos.{runid:03d}.pp1.nc'},
    matrix  = {'runid': [7, 240]},
)
def convert(inputs, outputs, runid):    # runid is the int 7, not '007'
    ...                                 # outputs['nc'] -> 'atmos.007.pp1.nc'
```

The task still gets the int; only the path is padded. The spec must suit the
value: `{runid:03d}` on the string `'240'` fails with `ValueError`. Use `{runid}`
for strings, or keep the padded strings in the matrix.

### Input order

The `inputs` and `outputs` dicts reach the rule function in the order you
declared them (or, for a callable, the order it returned them). Order is kept
through planning and through the job specs on SLURM. From 0.9.0, reordering the
keys of an `inputs` or `outputs` dict is not a reason to rerun a task, so
don't rely on the order surviving an edit. If order matters to your code
(say it takes a list of files), use sortable keys and sort by them:

```python
inputs = {f'in{i:03d}': f'data/part_{i}.nc' for i in range(n)}
...
paths = [inputs[k] for k in sorted(inputs)]
```

### Shared side outputs

Sometimes every task of a rule writes the same file — a per-rule summary
figure that each task overwrites. Do **not** declare it in `outputs` on every
task. Several tasks would then claim one path, which is the "shared outputs"
case: downstream tasks depend on every task of the rule, SLURM uses `afterok`
instead of `aftercorr`, and the file is only as good as the last task to
write it. Two options:

- **A separate fan-in rule that produces it.** Give the rule one task, list the
  per-task outputs as its `inputs`, and have it write the file. It is tracked,
  runs once after all the tasks, and reruns when any of them change. This is
  the right choice if anything reads the file.
- **Leave it undeclared.** The file is then untracked: remake doesn't check it
  exists, and doesn't rerun anything when it changes or goes missing. Fine for a
  throw-away diagnostic that nothing depends on.

## Output checks with `Ensure`

By default remake trusts a task that returns without raising. Wrap an output
in `Ensure` to check it when the task finishes; a failed check fails the task:

```python
from remake import Ensure, rule

@rule(
    inputs  = {'raw': 'data/raw/{year}.nc'},
    outputs = {'nc': Ensure('data/out/{year}.nc', min_size=1024, checksum=True)},
    matrix  = {'year': YEARS},
)
def process(inputs, outputs, year):
    ...
```

- `nonempty=True` (the default): the file exists and is not empty.
- `min_size=N`: at least `N` bytes.
- `checksum=True`: record the output's sha256 (shown by `remake task-info`).
  It re-reads every byte, so it is opt-in; `config={'checksum_outputs': True}`
  on a rule or in `remake.toml` turns it on for all outputs. Checksums are
  recorded, never compared: they are not a rerun trigger.

`Ensure` returns the same kind of output (a path becomes a `FileToken`), so
rule code uses it as before. Adding, removing or changing a check doesn't
rerun anything, and `check_outputs`/`set-state --check-outputs` apply the
checks before adopting a file. Plain outputs are not checked, as before.

## Rule factories

When many rules differ only in a few values, write a function that makes them.
Pass `name=` so each gets its own identity (they would all be called `run`
otherwise, which is a duplicate-name error):

```python
def stats_rule(analyser_name, group_names):
    @rule(
        name=f'stats_{analyser_name}',
        inputs=lambda group: {e: f'data/raw/{e}.txt' for e in GROUPS[group]},
        outputs={'stat': f'data/stats/{analyser_name}/{{group}}.txt'},
        matrix={'group': list(group_names)},
        uses={'analyser': ANALYSERS[analyser_name], 'groups': GROUPS},
    )
    def run(inputs, outputs, group):
        ...analyser.reduce(...groups[group]...)
    return run

stats_rules = [stats_rule(name, GROUPS) for name in ANALYSERS]
rmk.add_rules(stats_rules)
```

See `examples/ex13_rule_factory.py` for a complete pipeline. Three things to
watch.

**Generated rules share one function source.** Every rule from the factory
has the same body, so remake can't tell them apart by code. A change to the
body reruns all of them; a change to what makes one different (the analyser
class here) is seen only if it is in `uses`. Put everything that varies
between the generated rules into `uses`.

**Lambdas made in the factory capture values change detection can't see.**
The [closure warning](#which-upstream-tasks-a-task-depends-on) above applies:
a lambda hashes only its own source, not what it closed over. If the `inputs`
lambda above used `analyser_name` or a factory argument, changing that value
would not rerun anything. Either read only the matrix kwargs (and module-level
constants) in the lambda, or derive the paths from the kwargs, or put the value
in `uses`.

**Don't give a factory parameter the same name as a `uses` key.** In the rule
body a name resolves like any Python name, and the factory's variables are
closer than `uses`. If the factory is `def stats_rule(name, groups)` and the rule has
`uses={'groups': GROUPS}`, the body gets the factory's `groups` (a list of
names), not the dict from `uses`. The failure shows up only when a task runs.
From 0.9.0, remake warns at decoration when a `uses` key is shadowed by a
closure variable of an enclosing function. Name the factory parameters
differently (`group_names` above) and the warning, and the bug, go away.

## Directory outputs

A task that writes a variable number of files (a plot per detected event, a
tile per region that had data) can declare a directory as its output with
`Dir`. Every file in it is attributed to that task, and each `Dir` belongs to
exactly one task:

```python
from remake import Dir, Ensure

@rule(outputs={'plots': Ensure(Dir('data/plots/{year}'))}, matrix={'year': YEARS})
def find_peaks(outputs, year):
    (Path(outputs['plots']) / 'peak_0.png').write_bytes(...)   # the dir exists already
```

A `Dir` is a path, so rule code writes into it directly; remake creates it
before the rule runs. Downstream rules that need to know which files turned
up use a `@deferrable` matrix that lists the directory once the task is
complete (see `examples/ex14_dir_outputs.py`).

**Completion is a manifest, not the directory.** After the rule returns
successfully, remake scans the directory and writes a hidden
`.remake-manifest.json` into it (atomically): each file's relative path, size
and mtime, plus file and byte totals. The `Dir` is complete when the manifest
exists, so a task killed part-way (a directory with half its files) is never
mistaken for finished, and `--check-outputs` / `set-state --check-outputs`
never adopt it. `Ensure(Dir(...), nonempty=True, min_size=...)` fails the task
unless the directory holds at least one file (or `min_size` bytes in total);
`Ensure(Dir(...), checksum=True)` (or `checksum_outputs=True` in the config)
adds a sha256 per file to the manifest.

**remake never deletes your files.** When a task reruns, only the manifest is
removed (the directory is "incomplete" while the task runs). Files that the
latest run did not produce (written before it started, under a name that
already existed) are recorded as *stale* in the manifest, and remake warns
when it finds any. The recorded size is for the produced files only. Clearing
stale files is up to you. `remake task-info` shows the file and byte counts
and how many files are stale; `remake info --sizes` adds an output-size
column (and `info --json` always carries `output_bytes` per rule).

**Limitation: mtime-preserving copies.** To tell produced from stale, remake
compares each file's mtime with the task's start time, and also notes which
names were already present. A file written by `shutil.copy2`, `rsync -a` or
`cp -p` keeps its source's old mtime: if its name is new it is still counted
as produced, but if it *replaces* an existing name it looks stale and
triggers the warning. Write such files under new names (or use plain
`shutil.copy`) to avoid that.

**Don't wrap a `ZarrStore` in a `Dir`.** A `ZarrStore` is already a
directory output with its own completion marker (`.zmetadata`).

**Checks.** Two tasks declaring the same `Dir` is an error when you run (the
message names both tasks); `remake lint` also reports a `Dir` nested inside
another rule's `Dir`, a plain output file inside another rule's `Dir`, and
treats an input that lies inside another rule's `Dir` as produced by that
rule (flagging it as a missing dependency if the rule is not in `depends_on`).

**Converting an existing rule from N files to one `Dir`.** Changing a rule's
outputs is an io change, so the whole rule would rerun. If the files are
already on disk and good, edit the remakefile, then restamp and write the
manifests:

```
remake set-state pipeline.py -Q 'rule == "gen"' --success --check-outputs
```

`--check-outputs` scans an existing `Dir` that has no manifest and claims
everything in it as produced (a missing directory is skipped), and the
restamp records the new outputs definition, so nothing reruns.

## Registering rules

End the module with:

```python
rmk.rules_from_current_module()
```

This collects every `@rule` defined in the module onto the `Remake` object.
For pipelines split across modules, see `examples/ex5_multifile/`.

A scan claims a rule only if a module-level **name is bound directly to it**
and the rule is **defined in the scanned file**. Two kinds of rule are left
alone, and named together in one warning that says to register them with
`rmk.add_rules(...)`:

- a rule *imported* from another module (`from rules_extract import extract`).
  Register such rules explicitly, or scan their module with
  `rmk.rules_from_modules(rules_extract, rules_analysis)` (list a module after
  the ones it imports from, so its imports are already registered);
- a rule held in a list, tuple or dict (one level deep), including rules made
  in a loop or factory. These are never claimed, even when defined in this
  file, because a container is often a deliberate subset. A rule made in a
  loop whose name is rebound each time is still claimed once, for the last one.

```python
stats_rules = [stats_rule(name, GROUPS) for name in ANALYSERS]
rmk.add_rules(stats_rules)
rmk.rules_from_current_module()    # last: the list is already registered, no warning
```

Mixing the two is fine: rules registered by both routes are not duplicated.
Put `rules_from_current_module()` after any `add_rules` call, or the scan
warns about rules you are about to register.
