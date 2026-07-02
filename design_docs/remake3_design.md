# remake3 — Design Document

## Motivation

remake2 has served well for traditional file-based scientific pipelines, but three
interconnected problems have emerged at scale:

1. **Filename-based DAG wiring is fragile and slow.** Connecting rules by matching
   output path strings to input path strings works elegantly at small scale but
   silently misfires on typos, and materialising 1e6 path strings at load time is
   measurably slow. `pathlib.Path` construction at that scale is prohibitively
   expensive. Any option that touches the filesystem (existence checks, mtimes) is
   ruled out entirely for large matrices.

2. **The file model breaks for non-file outputs.** Zarr stores, S3 objects, and
   database tables are all natural outputs in modern scientific workflows. They
   cannot be addressed as single files, and mtime-based rerun logic is meaningless
   for them.

3. **Scope leakage is a silent correctness bug.** `rule_run` closures can
   capture module-level constants and helper functions that influence results.
   If those change, the AST comparison misses it and tasks are not rerun.

remake3 is a clean-break rewrite that addresses all three, while keeping what
works: SQLite-backed status tracking, smart code-change detection, matrix
expansion, and SLURM support.

---

## Core design principles

- **Decorator-based rule definition** — rules are plain functions decorated with
  the module-level `@rule(...)`, not classes used as namespaces. Decoration is
  decoupled from registration, so rules can be defined across many modules and
  combined in a top-level file.
- **Explicit registration, no global registry** — a pipeline is assembled by
  handing `Rule` objects to a `Remake` instance (`Remake(rules=[...])`,
  `Remake.from_modules(...)`, `Remake.from_current_module()`). Two independent
  `Remake` objects can coexist in one process; nothing leaks between tests.
- **CLI is a thin render layer over a complete Python API** — every operation
  the command line exposes is a method on `Remake` that returns structured
  data: `run`/`plan`, `status_summary` (info), `lint`, `why`/`explain_tasks`,
  `task_info`, `rule_dag`, `select_task` and `set_state`. `remake_cmd.py`
  (`RemakeCLI`) only parses args and renders that data as tables or `--json`;
  it holds no behaviour of its own. A pipeline can therefore be driven and
  introspected entirely from Python (e.g. a notebook), and the CLI stays
  trivially testable. The test for where code belongs: *would a programmatic
  caller want this back as a dict/list?* — if yes it is a `Remake` method, and
  only the terminal formatting lives in the CLI.
- **Explicit dependencies** — the rule-level DAG is declared via `depends_on`,
  not inferred from path strings.
- **Only declare what exists** — `inputs`, `outputs`, and `matrix` are all
  optional. A source rule takes no `inputs`; a side-effect rule (zarr region
  writes, database upserts) takes no `outputs`. The rule function's signature
  must match its declarations, checked at decoration time.
- **Lazy task expansion** — the rule-level DAG is built eagerly (it is small);
  individual `Task` objects are expanded on demand.
- **Completion is DB-tracked; output tokens are opt-in verification** — task
  completion is recorded in the metadata backend. Outputs, where declared, are
  token objects (`FileToken`, `ZarrStore`, `S3Object`, ...) with a common
  `is_complete()` interface used to reconcile or audit the DB against the
  world, not as the primary completion mechanism. Path-backed tokens are
  transparent (`os.PathLike`), so rule code uses them like plain paths.
- **Dependency injection throughout** — metadata backend, executor, and
  filesystem access are all injected, never hardwired.
- **Controlled scope** — external dependencies of `rule_run` must be declared
  in `uses`; undeclared free variables produce warnings (or errors in strict
  mode).
- **Testability as a first-class concern** — every subsystem is independently
  unit-testable; integration tests use `tmp_path` and in-memory SQLite.
- **Config file formats — YAML for humans, JSON for internals** — any
  config a user is expected to read or hand-edit (pipeline parameters,
  benchmark/run settings) is YAML (`pyyaml` is a dependency; use
  `yaml.safe_load`/`yaml.safe_dump`). JSON is fine — and preferred — for
  internal machine-written/machine-read artefacts and command output that
  is not meant to be edited by hand: the `.remake/jobs/*.json` specs and
  jobid sidecars, per-task result sidecars, and `remake ... --json` output.

---

## Scale target

The design scale (MM, 2026-07-02, from a real remakefile developed against
remake2) is **~1e4 tasks × ~1e2 files per task = ~1e6 files**, not 1e6
tasks. Field remake3 pipelines to date are 1e3–2e4 tasks; SLURM's
`MaxArraySize` (typically a few thousand to ~10k) makes a 1e6-element
submission impossible as one array anyway. The long-standing 1e6-*task*
figure (bench_million_tasks.py and the todos written around it) is a
**stress-headroom ceiling, not a requirement** — worth keeping green, but
not worth engineering effort on its own.

Consequences for prioritisation:

- **Per-task costs** (DB row writes/commits, planner per-task loop, Task
  object memory) need to be comfortable at 1e4 — which, post the 2026-07
  storage/query rework, they are with orders-of-magnitude headroom. Items
  framed as "matters at 1e6 tasks" (e.g. batching the per-completion
  EXCLUSIVE transaction) are correctness-of-shape improvements, not
  urgent.
- **Per-file costs are the real scaling frontier**: input/output
  resolution builds ~1e2 token objects per task touch; `check_outputs`
  and `lint`-style paths stat or materialise up to 1e6 paths — minutes on
  a parallel cluster filesystem (Lustre/NFS) even when local ext4 looks
  fine. New work should be benchmarked against this shape
  (`tests/benchmarks/bench_field_scale.py`: 1e4 tasks × 100 outputs).

---

## API overview

### Defining a pipeline

```python
# climate_pipeline.py
from pathlib import Path
from remake import Remake, ZarrStore, rule

rmk = Remake()

MODELS = ['era5', 'cmip6_hist', 'cmip6_rcp85']
YEARS  = list(range(1980, 2021))

# --- rule 1: extract ---

def extract_inputs(model, year):
    """Callable inputs — full Python, no restrictions."""
    base = 'data/raw/era5' if model == 'era5' else f'data/raw/{model}'
    return {'raw': f'{base}/{year}.nc'}

@rule(
    inputs  = extract_inputs,
    outputs = {'clean': 'data/clean/{model}/{year}.nc'},
    matrix  = {'model': MODELS, 'year': YEARS},
)
def extract(inputs, outputs, model, year):
    import xarray as xr
    ds = xr.open_dataset(inputs['raw'])
    ds.to_netcdf(outputs['clean'])


# --- rule 2: compute anomalies (depends on extract) ---

@rule(
    inputs  = extract.outputs,          # rule object carries its output spec
    outputs = {'zarr': ZarrStore('data/anomalies/{model}/{year}.zarr')},
    matrix  = extract.matrix,           # inherit matrix
    depends_on = [extract],
)
def anomalies(inputs, outputs, model, year):
    import xarray as xr
    ds = xr.open_dataset(inputs['clean'])
    clim = ds.mean('time')
    (ds - clim).to_zarr(outputs['zarr'])    # tokens are path-like


# --- rule 3: aggregate all years per model (fan-in) ---

def agg_inputs(model):
    return {str(year): f'data/anomalies/{model}/{year}.zarr' for year in YEARS}

@rule(
    inputs     = agg_inputs,
    outputs    = {'agg': ZarrStore('data/aggregated/{model}.zarr')},
    matrix     = {'model': MODELS},
    depends_on = [anomalies],
)
def aggregate(inputs, outputs, model):
    import xarray as xr
    ds = xr.open_mfdataset(list(inputs.values()), engine='zarr')
    ds.to_zarr(outputs['agg'])


rmk.rules_from_current_module()
```

### Registering rules with a Remake instance

`@rule(...)` is a module-level decorator producing a free-standing `Rule`
object; it does not register anything. The `Remake` instance is created at
the top of the file (so config options stay visible up front) and rules are
registered with it in one of three ways:

```python
# 1. Collect every Rule visible in the calling module — the common case.
#    Goes at the END of the file, after all rule definitions.
rmk.rules_from_current_module()

# 2. Collect every Rule defined in given modules
import rules_extract, rules_analysis
rmk.rules_from_modules(rules_extract, rules_analysis)

# 3. Explicit list — clearest for tests and small pipelines
rmk = Remake(rules=[extract, anomalies, aggregate])
# equivalently: rmk.add_rules([extract, anomalies, aggregate])
```

Because rules are plain importable objects, a pipeline can be split across
files and combined at the top level — `inputs=extract.outputs`,
`matrix=extract.matrix` and `depends_on=[extract]` are ordinary attribute
accesses on the imported `Rule`:

```python
# pipeline.py
from remake import Remake
from rules_extract import *      # extract
from rules_analysis import *     # anomalies, aggregate

rmk = Remake()
rmk.rules_from_current_module()  # sees imported rules too
```

`rules_from_current_module()` is implemented by scanning the caller's
globals:

```python
def rules_from_current_module(self) -> None:
    caller_globals = inspect.currentframe().f_back.f_globals
    self.add_rules(
        v for v in caller_globals.values() if isinstance(v, Rule)
    )
```

Notes on its behaviour:

- It must be called **after** all rule definitions — i.e. it goes at the
  end of the file, while `rmk = Remake(...)` stays at the top.
- It picks up `Rule` objects *imported into* the module as well as those
  defined in it. This is the multi-file composition mechanism, not a bug.
- Definition order is preserved (module `__dict__` is insertion-ordered),
  and `add_rules` deduplicates by identity.
- There is no global registry: a module defining rules can be imported by
  two different pipelines, and two `Remake` instances can coexist in one
  process (the in-memory-SQLite test pattern relies on this).

Remake-level defaults (`strict_scope`, `config`) are resolved at
registration time: rule-level settings are tri-state (`None` = inherit),
and `Remake` fills in its defaults when the rule is registered.

### Optional `inputs`, `outputs` and `matrix` — the signature contract

All three declarations default to `None` and may be omitted:

- **No `inputs`** — source rules: downloads, data generators, directory
  scanners. Nothing upstream feeds them.
- **No `outputs`** — side-effect rules: writes to a region of a zarr store,
  upserts to a database table, POSTs to an API. There is no independently
  addressable artefact, and inventing dummy sentinel files to satisfy the
  framework is exactly the kind of noise remake3 avoids. Completion is
  DB-tracked (see *Output tokens* below), so an output-less rule has
  well-defined rerun semantics, can be listed in `depends_on`, and works
  with SLURM continuation jobs — none of that machinery reads outputs.
- **No `matrix`** — the rule expands to exactly one task.

The rule function's signature must mirror what is declared. The expected
signature is:

```
def my_rule([inputs,] [outputs,] <one parameter per matrix key>):
```

`@rule` validates this with `inspect.signature` at decoration time:
declaring `inputs` but omitting the parameter (or vice versa) is an
immediate error, as is a missing or typo'd matrix parameter (`yaer` for
`year` fails at import, not as a `TypeError` inside a SLURM job three hours
later). `inputs={}` / `outputs={}` are rejected as ambiguous — omit the
argument instead. A callable spec counts as declared even if it returns an
empty dict for some kwargs.

A pipeline writing regions of a shared zarr store:

```python
@rule(outputs={'store': ZarrStore('data/big.zarr')}, ...)
def create_store(outputs):
    # source rule: no inputs. Initialises an empty store with the full
    # coordinate grid, then consolidates metadata.
    ...

@rule(
    inputs     = create_store.outputs,
    matrix     = {'year': YEARS},
    depends_on = [create_store],
)
def write_region(inputs, year):
    # side-effect rule: no outputs. Writes one year's slab into the store.
    ds = compute_year(year)
    ds.to_zarr(inputs['store'], region={'time': year_slice(year)})

@rule(matrix={'table': TABLES})
def refresh_views(table):
    # neither inputs nor outputs: pure side effect, one task per table
    ...
```

Two consequences of output-less rules, stated so they are not surprises:

1. **The DB is the only record of completion.** If the metadata DB is lost,
   file-backed rules can be reconciled via output tokens; output-less rules
   will rerun. For idempotent region writes and upserts this is wasteful
   but harmless.
2. **There is nothing to audit.** `check_outputs` modes (below) treat
   output-less tasks as DB-authoritative. Where the side effect *is*
   cheaply checkable, a custom `OutputToken` (e.g. one that checks a
   sentinel zarr attribute or queries for a DB row) restores verification —
   opt-in, never required.

### Using `uses` for tracked external dependencies

```python
THRESHOLD = 0.5                          # module-level constant

@rule(
    inputs  = {'raw': 'data/raw/{year}.csv'},
    outputs = {'filt': 'data/filtered/{year}.csv'},
    matrix  = {'year': YEARS},
    uses    = {'threshold': THRESHOLD},  # remake3 tracks this value
)
def filter_data(inputs, outputs, year):
    import pandas as pd
    df = pd.read_csv(inputs['raw'])
    df[df.value > threshold].to_csv(outputs['filt'])
```

If `THRESHOLD` is changed to `0.7`, remake3 detects the change and marks all
`filter_data` tasks as requiring rerun — even though `rule_run`'s AST is
identical. Functions in `uses` are compared by AST; plain values by `repr()`.
(A user-supplied hash hook may be added later; until then, avoid values with
large or unstable reprs in `uses`.)

Tracking is one level deep: a function in `uses` is compared by its own
body only. If it calls a further helper and *that* changes, nothing fires —
declare the helper in `uses` too. (`uses` values are injected into the rule
function's globals at execution time, so rule code refers to them directly
by name.)

### Strict scope mode

```python
rmk = Remake(strict_scope=True)
# or per-rule:
@rule(..., strict_scope=True)
def my_rule(inputs, outputs, year): ...
```

In strict mode, if `rule_run` accesses any name from outer scope that is not
covered by `uses`, a `ScopeError` is raised. In the default mode
(`strict_scope=False`) this is a warning.

The rule-level setting is tri-state: `strict_scope=None` (the default)
inherits the Remake-level setting. Timing follows from that: scope
*analysis* and warnings happen at decoration time (the free-variable
inspection needs no `Remake`); a rule-level `strict_scope=True` also errors
at decoration time, while the Remake-level default is enforced at
registration time — which is where the pipeline is assembled, and still
import time for the pipeline file.

Detection uses `fn.__code__.co_names` (globals accessed) and
`fn.__code__.co_freevars` (closure variables), filtered against a known-safe
list (stdlib modules, builtins).

---

## Architecture

```
remake/
├── core/
│   ├── remake.py          # Remake class — wires everything together
│   ├── rule.py            # Rule descriptor (produced by @rule)
│   ├── task.py            # Task dataclass — lightweight, lazy
│   ├── dag.py             # Rule-level DAG construction (pure functions)
│   ├── planner.py         # Decides which tasks need running (pure)
│   ├── scope.py           # Free-variable analysis, uses tracking
│   └── tokens.py          # OutputToken ABC + FileToken/ZarrStore/S3Object
├── metadata/
│   ├── metadata_manager.py # MetadataManager ABC, TaskRecord, statuses
│   ├── sqlite3_backend.py  # Production backend (+ sidecar ingestion)
│   └── sidecar.py          # SidecarWriter — per-task result files
├── executors/
│   ├── executor.py        # Executor ABC
│   ├── singleproc_executor.py
│   ├── multiproc_executor.py
│   ├── slurm_executor.py  # Submits array jobs where possible
│   └── dask_executor.py
├── loader/                # load_module / load_remake
├── util/                  # code_compare (AST comparison), CLI arg helpers
└── remake_cmd.py          # CLI entry point — thin wrapper over Remake
```

### The `Rule` descriptor

`@rule(...)` returns a `Rule` object, not the original function. It is
free-standing — importable from any module, registered with a `Remake`
later. This object carries:

```python
@dataclass
class Rule:
    fn:         Callable                     # the rule_run function
    inputs:     dict | Callable | None       # spec, callable → spec, or absent
    outputs:    dict | Callable | None       # spec, callable → spec, or absent
    matrix:     dict | list | Callable | None  # None → single task
    depends_on: list[Rule]                   # explicit upstream rules
    uses:       dict                         # tracked external names/values
    strict_scope: bool | None                # None → inherit Remake default
    config:     dict                         # executor overrides (e.g. slurm mem)
    # set by Remake after registration:
    remake:     Remake | None = None
```

Decoration validates the signature contract (see above) and runs scope
analysis; registration resolves tri-state settings against Remake defaults
and wires the rule into the DAG.

`Rule.inputs` and `Rule.outputs` are always stored as-is (dict or callable) and
resolved per-task only when expansion is needed. This means **no path strings
are constructed at load time** for the full matrix.

### The `Task` dataclass

Tasks are not created until needed. A task is identified by `(rule, kwargs)`:

```python
@dataclass
class Task:
    rule:   Rule
    kwargs: dict       # e.g. {'model': 'era5', 'year': 1980}

    @cached_property
    def key(self) -> str:
        return sha1(f'{self.rule.fn.__name__}:{self.kwargs!r}'.encode()).hexdigest()

    @cached_property
    def inputs(self) -> dict:
        return _resolve(self.rule.inputs, self.kwargs)

    @cached_property
    def outputs(self) -> dict:
        return _resolve(self.rule.outputs, self.kwargs)
```

`inputs` and `outputs` are `cached_property` — resolved once, on first access,
not at construction time. For 1e6 tasks you only pay for the ones you touch.

### DAG construction (pure)

`dag.py` contains plain functions with no side effects:

```python
def build_rule_dag(rules: list[Rule]) -> nx.DiGraph:
    """Given a list of Rule objects, return a directed rule-level DAG."""
    g = nx.DiGraph()
    for rule in rules:
        g.add_node(rule)
        for dep in rule.depends_on:
            g.add_edge(dep, rule)
    assert nx.is_directed_acyclic_graph(g)
    return g

def expand_rule(rule: Rule) -> list[Task]:
    """Expand the matrix for one rule into Task objects (no I/O).

    list[dict] is the canonical internal format. The {key: [values]}
    cartesian shorthand is normalised to list[dict] here.
    May raise Defer if matrix is a @deferrable callable whose required
    upstream outputs do not yet exist.
    """
    kwargs_list = _resolve_matrix(rule.matrix)  # always returns list[dict]
    return [Task(rule=rule, kwargs=kw) for kw in kwargs_list]


def _resolve_matrix(matrix) -> list[dict]:
    """Normalise all matrix forms to list[dict]."""
    if callable(matrix):
        return matrix()   # @deferrable: raises Defer if not yet resolvable
    if isinstance(matrix, list):
        return matrix     # already list[dict]
    # {key: [values]} cartesian shorthand
    combos = list(itertools.product(*matrix.values()))
    return [dict(zip(matrix.keys(), combo)) for combo in combos]
```

`list[dict]` is the canonical internal format throughout — `{key: [values]}`
is syntactic sugar. This matters for dynamic matrices where combinations are
not a cartesian product (e.g. year 1980 has 3 clusters, year 1981 has 7).

Because these are pure functions they are trivially unit-testable.

### No task-level DAG — and a known limitation

The rule-level DAG is the only graph remake3 builds. Task-level ordering is
derived from it: rules run in topological order, and within a rule, tasks
form an independent, embarrassingly-parallel wave. Rerun propagation is
element-wise by kwargs when a rule shares its upstream's matrix, and
conservative (any upstream rerun marks all downstream tasks) otherwise —
over-rerunning is possible in odd matrix relationships, but never
under-rerunning. On SLURM, `aftercorr`/`afterok` express exactly these two
patterns natively.

The deliberate consequence: **intra-rule task dependencies are
inexpressible**. A rule whose task for `year` consumes the same rule's
output for `year - 1` (sequential time-stepping) cannot be modelled — there
is no task graph to hold that edge. The workarounds are to split the chain
into separate rules or to loop inside a single task. If real support is
ever needed it would be added as an explicit per-rule declaration (e.g.
`task_depends_on=lambda kwargs: ...`), not by reintroducing global
task-graph construction.

**Measured payoff.** The flip side of that limitation is graph cost scaling
with *rules*, not *tasks*. A synthetic head-to-head against Snakemake (a
two-stage 20,000-task / 100,000-file pipeline of trivial work; see the
`remake_vs` repo's `scale/` benchmark, 2026-06-15, one machine, local ext4)
makes it concrete: planning the full graph (dry-run) took remake **1.25 s /
79 MB** versus Snakemake's **40.8 s / 586 MB** — Snakemake materialises every
job up front, remake does not. At a smaller completable size (4,000 tasks)
the cold build was 18× faster and the no-op rebuild 22× faster; at full size
remake's cold build finished in 48 s while Snakemake's had not completed in
70+ min (its per-job scheduler overhead is superlinear). The numbers are
illustrative — trivial tasks deliberately expose framework overhead that
amortises away for minutes-long work — but they confirm the lazy,
rule-level model behaves as designed at scale.

This was measured on a laptop SSD; on JASMIN the gap would be *wider*. The
workload is metadata-bound (creating and statting 100k files), and a Lustre
group workspace has far higher metadata latency than a local SSD — so both
the absolute times and the gap grow, since the more file-stat-heavy tool
(Snakemake here, or remake's own `check_outputs='fallback'` path) is hit
hardest. This is the same parallel-filesystem stat cost flagged under
Performance/scaling in [todos.md](todos.md).

### The planner (pure)

```python
def plan(
    rules:    list[Rule],
    dag:      nx.DiGraph,
    metadata: MetadataManager,
    query:    str | None = None,   # eval'd vs task kwargs + rule name
    force:    bool = False,
    check_outputs: str = 'never',
    ignore_code_changes: bool = False,
) -> tuple[list[Task], list[Rule]]:
    """
    Return (runnable_tasks, deferred_rules).
    runnable_tasks: ordered list of tasks that need running now.
    deferred_rules: rules whose @deferrable matrix raised Defer.
    Pure except for metadata reads (injected).
    """
```

The planner reads task status from the metadata backend and returns a flat
ordered list plus any rules that could not yet be expanded. It never writes
to the DB, never touches the filesystem. The returned tasks are passed to an
executor; deferred rules are retried after the current wave completes.

---

## SQLite backend

The schema is largely carried over from remake2, with one addition: a `uses_hash`
column on `task` to track the serialised state of the `uses` dict.

```sql
CREATE TABLE code (
    id   INTEGER PRIMARY KEY,
    code TEXT NOT NULL
);

CREATE TABLE rule (
    id             INTEGER PRIMARY KEY,
    name           VARCHAR(200) NOT NULL,
    inputs_code_id  INTEGER REFERENCES code(id),
    outputs_code_id INTEGER REFERENCES code(id),
    run_code_id     INTEGER REFERENCES code(id)
);

CREATE TABLE task (
    id                 INTEGER PRIMARY KEY,
    key                VARCHAR(40) NOT NULL,   -- sha1 of (rule_name, kwargs)
    rule_id            INTEGER REFERENCES rule(id),
    run_code_id        INTEGER REFERENCES code(id),
    uses_hash          TEXT,                   -- repr/hash of uses values
    last_run_timestamp TIMESTAMP,
    last_run_status    INTEGER,                -- 0=pending, 1=ok, 2=failed
    exception          TEXT
);

CREATE UNIQUE INDEX task_key_index ON task(key);
```

**Change detection** at `finalize()` time:

1. Load current `run_code_id` and `uses_hash` for each task from the DB.
2. AST-compare the stored `rule_run` source against the current source.
3. Compare stored `uses_hash` against `repr(uses)` (or custom hash).
4. If either differs → `requires_rerun = True`.
5. Propagate: if any upstream task `requires_rerun`, so does this one.

The metadata manager is always injected:

```python
rmk = Remake(metadata=Sqlite3Backend('.remake/remake.db'))
# or for tests:
rmk = Remake(metadata=Sqlite3Backend(':memory:'))
```

The `MetadataManager` ABC defines:

```python
class MetadataManager(ABC):
    @abstractmethod
    def ensure_rules(self, rules) -> None: ...
    @abstractmethod
    def get_tasks_status(self, tasks: list[Task]) -> dict[str, TaskRecord]: ...
    @abstractmethod
    def update_task(self, task: Task, status: int, exception: str = '') -> None: ...
    # Concrete defaults, overridable: update_tasks (bulk), delete_tasks,
    # ingest_sidecars (fold per-task result files into the store — how
    # concurrent array/pool workers record results without touching it).
```

---

## SLURM executor

### User-facing interface

From the user's perspective there is one command:

```bash
remake run mypipeline.py --executor slurm
```

Internally this is a two-stage process — generate job specs and submit scripts,
then execute the master submit script — but that split is an implementation
detail, not a workflow the user has to manage.

Two additional commands are available:

```bash
remake run mypipeline.py --executor slurm --dry-run
# Stage 1+2 only: writes .remake/jobs/ and .remake/submit.sh without submitting.
# Useful for inspecting what would be submitted before touching the cluster.

remake resubmit mypipeline.py
# Re-executes .remake/submit.sh directly, skipping planning entirely.
# Useful when the cluster goes down mid-run and jobs need resubmitting.
```

### Stage 1 — job specs

The planner produces a list of tasks to run. The SLURM executor writes one JSON
file per rule under `.remake/jobs/<rule_name>.json`, containing an array of task
objects. The SLURM array index is simply the index into this array.

`.remake/jobs/extract.json`:
```json
[
    {"task_key": "abc123...", "rule": "extract", "kwargs": {"model": "era5", "year": 1980}},
    {"task_key": "def456...", "rule": "extract", "kwargs": {"model": "era5", "year": 1981}},
    ...
]
```

This keeps the file count at O(N_rules), not O(N_tasks) — important on
filesystems with inode limits (e.g. JASMIN).

Job IDs recorded after submission are kept in a separate lightweight sidecar
file per rule, `.remake/jobs/<rule_name>.jobids.json`, rather than rewriting the
potentially large task array:

```json
{"slurm_array_job_id": "1234567"}
```

These files persist across runs and are the authoritative record of what was
submitted. They are overwritten when a rule is resubmitted.

### Stage 2 — SLURM scripts

One `.sbatch` script is written per rule. For array-eligible rules the script is
parameterised by `$SLURM_ARRAY_TASK_ID`; for individual-job rules by the
spec index passed as a script argument at submission time.

**Array script** (rule is array-eligible, see below):

```bash
#!/bin/bash
#SBATCH --job-name=extract
#SBATCH --array=0-40
#SBATCH -o .remake/slurm/output/extract/%a.out
#SBATCH -e .remake/slurm/output/extract/%a.err
{extra_opts}

echo "SLURM RUNNING extract $SLURM_ARRAY_TASK_ID"
remake run-array-task mypipeline.py extract $SLURM_ARRAY_TASK_ID
echo "SLURM COMPLETED extract $SLURM_ARRAY_TASK_ID"
```

The script is intentionally dumb — it passes its rule name and array index
to `remake run-array-task`, which reads the spec at that index from
`.remake/jobs/extract.json` and constructs the task directly
(`task_from_spec`, no key search). No task-specific information needs to be
embedded in the job name or comment, which sidesteps the array job
identification problem entirely. The array process records its result as a
sidecar file (`.remake/tasks/results/...`), never a DB write — hundreds of
concurrent writers livelock SQLite on shared filesystems; the next
plan()/info ingests pending sidecars in one transaction.

**Master submit script** `.remake/submit.sh`:

```bash
#!/bin/bash
# Generated by remake3 — re-run to resubmit without replanning.

JOB_extract=$(sbatch --parsable .remake/slurm/extract.sbatch)
echo "{\"slurm_array_job_id\": \"$JOB_extract\"}" > .remake/jobs/extract.jobids.json

JOB_anomalies=$(sbatch --parsable \
    --dependency=aftercorr:$JOB_extract \
    .remake/slurm/anomalies.sbatch)
echo "{\"slurm_array_job_id\": \"$JOB_anomalies\"}" > .remake/jobs/anomalies.jobids.json

JOB_aggregate=$(sbatch --parsable \
    --dependency=afterok:$JOB_anomalies \
    .remake/slurm/aggregate.sbatch)
echo "{\"slurm_array_job_id\": \"$JOB_aggregate\"}" > .remake/jobs/aggregate.jobids.json
```

`--parsable` makes `sbatch` output only the job ID for easy capture.

### Stage 3 — submission

`remake run --executor slurm` executes `.remake/submit.sh` immediately after
generating it, unless `--dry-run` is given.

### Detecting already-running tasks

Instead of relying on job names in `squeue`, remake3 reads
`.remake/jobs/<rule>.jobids.json` for the last known job ID, then checks
`squeue` for that job ID and array index. A task is considered already
queued/running if its array job ID appears in `squeue` with status `PD` or `R`
at the corresponding array index.

### Array job eligibility

```
use_array = True
  if rule has no intra-rule task dependencies
  and all depends_on rules use the same matrix  →  aftercorr applies
  and matrix size >= configurable threshold (default: 10)
```

`aftercorr:JOBID` wires array element N of the downstream job to element N of
the upstream job — `anomalies[era5,1981]` waits only for `extract[era5,1981]`,
not the entire extract array.

When a fan-in occurs (e.g. `aggregate` collecting all years), the downstream
rule is not array-eligible and falls back to individual jobs with
`--dependency=afterok:JOBID_1:JOBID_2:...` listing the relevant upstream array
element IDs.

### Per-rule SLURM config

```python
@rule(
    inputs  = extract_inputs,
    outputs = {'clean': 'data/clean/{model}/{year}.nc'},
    matrix  = {'model': MODELS, 'year': YEARS},
    config  = {'slurm': {'mem': '16G', 'time': '2:00:00', 'partition': 'himem'}},
)
def extract(inputs, outputs, model, year): ...
```

Global defaults come from `Remake(config={'slurm': {...}})`. Per-rule config is
merged on top of global defaults and written into the rule's `.sbatch` script.

---

## Dynamic matrices

### The problem

Not all task counts are known at load time. Common cases:

- A clustering rule produces N clusters; N is unknown until the algorithm runs
- An event-detection rule finds M events in a time series
- A QC rule filters an ensemble; only passing members proceed
- An input rule scans a directory; processes whatever files are present

These cannot be expressed as a static `matrix` dict defined at decoration time.

### API

`matrix` accepts a callable that returns `list[dict]`. The callable is called
during planning; mark it `@deferrable` and, if the required upstream outputs
do not yet exist, raise `Defer`, which the planner treats as a deferral signal
rather than an error. The planner also defers a `@deferrable` rule while an
upstream is rerunning, so its matrix never expands from a stale output.

```python
from remake import Remake, deferrable, rule
import json
from pathlib import Path

rmk = Remake()
YEARS = list(range(1980, 2021))


@rule(
    inputs  = {'raw': 'data/raw/{year}.nc'},
    outputs = {'clusters': 'data/clusters/{year}.json'},
    matrix  = {'year': YEARS},
)
def cluster(inputs, outputs, year):
    # Writes a JSON list of discovered cluster IDs — count unknown upfront
    ids = run_clustering(inputs['raw'])
    Path(outputs['clusters']).write_text(json.dumps(ids))


@deferrable
def process_matrix():
    """Called during planning, after cluster has run."""
    rows = []
    for year in YEARS:
        path = Path(f'data/clusters/{year}.json')
        if not path.exists():
            raise Defer(str(path))
        for cid in json.loads(path.read_text()):
            rows.append({'year': year, 'cluster_id': cid})
    return rows   # list[dict] — not a cartesian product


@rule(
    inputs     = lambda year, cluster_id: {
        'data': f'data/clusters/{year}/{cluster_id}.nc'
    },
    outputs    = {'result': 'data/results/{year}/{cluster_id}.nc'},
    matrix     = process_matrix,
    depends_on = [cluster],
)
def process_cluster(inputs, outputs, year, cluster_id):
    ...


rmk.rules_from_current_module()
```

`Defer` accepts one or more path strings as context, which remake3
surfaces in its output so the user knows what is blocking resolution.

### Execution model — local executors

For singleproc and multiproc executors, the executor drives a replanning loop
internally. From the user's perspective, `remake run` is still a single
command:

```
loop:
    runnable, deferred = plan(rules, dag, metadata)
    if not runnable and not deferred: break          # fully complete
    if not runnable and deferred:
        report_blocked(deferred); break              # stuck — upstream failed
    execute(runnable)
    # deferred rules may now be resolvable — continue loop
```

After each wave of tasks completes, the planner retries all deferred rules. If
a rule remains deferred despite its `depends_on` tasks all completing, remake3
reports it as blocked (the matrix callable raised `Defer` but all
upstream tasks are done — likely a bug in the callable or a missing output).

### Execution model — SLURM executor

A Python process cannot be kept alive between SLURM job waves. Instead, the
SLURM executor submits a lightweight **continuation job** after any rule with a
dynamic matrix:

```bash
# submit.sh (generated)
JOB_cluster=$(sbatch --parsable .remake/slurm/cluster.sbatch)
echo "{\"slurm_array_job_id\": \"$JOB_cluster\"}" > .remake/jobs/cluster.jobids.json

# Continuation: reruns remake3 after cluster completes.
# remake3 will resolve process_matrix, write process_cluster.json,
# generate process_cluster.sbatch, and submit it.
sbatch --dependency=afterok:$JOB_cluster \
       --job-name=remake_continue \
       .remake/slurm/continuation.sbatch
```

`.remake/slurm/continuation.sbatch`:
```bash
#!/bin/bash
#SBATCH --mem=1G --time=00:10:00 --partition=standard --qos=standard
remake run mypipeline.py --executor slurm
```

The continuation job is cheap (planning + submission only, no computation).
`remake run` is idempotent — already-complete tasks are skipped. Arbitrarily
deep chains of dynamic rules are handled naturally: each invocation emits
another continuation job if further deferred rules remain.

The `.remake/jobs/process_cluster.json` file is written by the continuation job,
not the initial submission. The SLURM array for `process_cluster` is submitted
at that point with the correct size. The initial `submit.sh` does not reference
`process_cluster` at all.

### Task key stability

Task keys are `sha1(f'{rule_name}:{kwargs!r}')`. Because kwargs values are
stable (e.g. `{'year': 1980, 'cluster_id': 'c42'}` is always the same string
regardless of when it was discovered), DB entries for previously-completed
dynamic tasks are found and reused correctly across replanning runs. Adding new
cluster IDs in a subsequent run only creates new task entries; existing ones are
untouched.

### Dynamic matrices and SLURM array eligibility

A rule with a dynamic matrix **cannot** be submitted as an array job from the
initial `submit.sh` because its size is not yet known. It is always submitted
from within a continuation job. Array eligibility rules (same matrix as
upstream, no intra-rule deps) still apply once the matrix is resolved — the
continuation job submits an array if eligible.

---

## Output tokens

Task completion is tracked in the metadata DB — that is the primary
mechanism, and it works for rules with no outputs at all. Output tokens are
the **opt-in verification layer**: a way to ask the world, rather than the
DB, whether an output exists in finished form. Each token type encodes its
own definition of *finished* — that knowledge lives in the token, once, not
scattered across rules.

Declared outputs are `OutputToken` instances. String paths are automatically
wrapped in `FileToken`, so the common case needs no token syntax at all.

```python
class OutputToken(ABC):
    @abstractmethod
    def identity(self) -> str:
        """Stable string identifying this output (display, hashing)."""

    @abstractmethod
    def is_complete(self) -> bool:
        """Has this output been successfully produced?"""

    def __str__(self) -> str:
        return self.identity()


class PathToken(OutputToken):
    """Base for path-backed tokens. Transparent: implements __fspath__,
    so rule code passes tokens straight to open(), Path(), xarray, zarr —
    no .path unwrapping."""

    def __init__(self, path: str):
        self.path = path

    def __fspath__(self) -> str:
        return self.path

    def identity(self) -> str:
        return self.path


class FileToken(PathToken):
    def is_complete(self) -> bool:
        return Path(self.path).exists()


class ZarrStore(PathToken):
    def is_complete(self) -> bool:
        # A half-written store also has a directory; only the consolidated
        # metadata written by zarr.consolidate_metadata() marks completion.
        return Path(self.path, '.zmetadata').exists()


class S3Object(OutputToken):
    # Not path-like: str(token) gives the URI.
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key

    def identity(self) -> str:
        return f's3://{self.bucket}/{self.key}'

    def is_complete(self) -> bool:
        import boto3
        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except s3.exceptions.ClientError:
            return False
```

### Parent directory creation

Before a task runs, remake3 resolves its outputs and creates the parent
directory of every path-backed token (`mkdir -p` semantics). Rule code
never needs `Path(outputs[...]).parent.mkdir(parents=True, exist_ok=True)`
boilerplate. For a `ZarrStore` the parent of the store directory is
created — the store itself is written by zarr. Non-path tokens
(`S3Object`, ...) are unaffected.

### When `is_complete()` is consulted

`Remake(check_outputs=...)` selects one of three modes:

- **`'never'`** (default) — DB is the sole source of truth. Fastest; no
  I/O beyond the DB at plan time. A task with no DB record always reruns,
  even if its outputs exist on disk — so an edit after `set-state
  --pending` is never silently swallowed by re-adopting a stale output
  (the iterative-development trap that `'fallback'`-as-default created).
- **`'fallback'`** — tokens are consulted only for tasks the DB has no
  record of. This makes a lost or absent DB recoverable: completed
  file-backed work is recognised from its outputs instead of rerun.
  Per-run cost is zero once the DB is populated. This is the **migration**
  mode — adopt a pre-existing output tree into a fresh `.remake/` — and is
  opt-in rather than the default. The explicit, persistent adoption path
  is `set-state -Q <query> --success --check-outputs`.
- **`'always'`** — every planned task's outputs are checked. Detects
  outputs deleted behind the DB's back — e.g. scratch-filesystem purges —
  at the cost of touching the filesystem (or S3) for every output. Also
  available per-invocation as `remake run --check-outputs` (which also
  adopts complete outputs with no DB record, like `'fallback'`).

In every mode, tasks with no declared outputs are DB-authoritative: there
is nothing to check, and that is fine.

---

## Testability

### Dependency injection points

| Concern          | Injected via                          | Test substitute                    |
|------------------|---------------------------------------|------------------------------------|
| Metadata storage | `Remake(metadata=...)`                | `Sqlite3Backend(':memory:')`       |
| Executor         | `rmk.run(executor=...)`               | `SyncExecutor()` (runs inline)     |
| Output tokens    | `is_complete()` method                | `AlwaysComplete()` / `NeverComplete()` |
| Filesystem       | `FileToken.is_complete()`             | Monkeypatch or token substitution  |

### Test layers

**Unit tests** — pure functions, no I/O:

```python
def test_build_rule_dag():
    @rule(outputs={'a': 'a.txt'})
    def rule_a(outputs): pass

    @rule(inputs=rule_a.outputs, outputs={'b': 'b.txt'},
          depends_on=[rule_a])
    def rule_b(inputs, outputs): pass

    dag = build_rule_dag([rule_a, rule_b])
    assert list(nx.topological_sort(dag)) == [rule_a, rule_b]


def test_matrix_expansion():
    rule = make_rule(matrix={'model': ['a', 'b'], 'year': [2000, 2001]})
    tasks = expand_rule(rule)
    assert len(tasks) == 4
    assert {t.kwargs['model'] for t in tasks} == {'a', 'b'}


def test_planner_reruns_on_code_change(tmp_path):
    meta = Sqlite3Backend(':memory:')

    @rule(outputs={'out': str(tmp_path / '{x}.txt')}, matrix={'x': [1]})
    def my_rule(outputs, x):
        pass

    rmk = Remake(rules=[my_rule], metadata=meta)
    runnable, deferred = rmk.plan()
    assert len(runnable) == 1  # never run

    # Simulate a completed run
    meta.update_task(runnable[0], TASK_STATUS_SUCCESS)

    # Mutate the source
    my_rule.fn = lambda outputs, x: print('changed')
    runnable, deferred = rmk.plan()
    assert len(runnable) == 1  # code changed
```

**Integration tests** — real filesystem via `tmp_path`, in-memory DB:

```python
@pytest.fixture
def rmk():
    return Remake(metadata=Sqlite3Backend(':memory:'))

def test_two_rule_pipeline(rmk, tmp_path):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'raw' / 'data.txt').write_text('hello')

    @rule(
        inputs  = {'src': str(tmp_path / 'raw/data.txt')},
        outputs = {'dst': str(tmp_path / 'out/data.txt')},
    )
    def process(inputs, outputs):
        # parent dir of outputs['dst'] is created by remake before the run
        Path(outputs['dst']).write_text(Path(inputs['src']).read_text().upper())

    rmk.add_rules([process])
    rmk.run()
    assert Path(tmp_path / 'out/data.txt').read_text() == 'HELLO'
```

**Scope warning tests:**

```python
def test_scope_warning_on_free_variable():
    # Scope analysis happens at decoration — no Remake needed.
    CONSTANT = 42
    with pytest.warns(ScopeWarning, match='CONSTANT'):
        @rule(outputs={'out': 'out.txt'})
        def my_rule(outputs):
            return CONSTANT   # closes over CONSTANT, not declared in uses

def test_scope_error_rule_level_strict():
    CONSTANT = 42
    with pytest.raises(ScopeError):
        @rule(outputs={'out': 'out.txt'}, strict_scope=True)
        def my_rule(outputs):
            return CONSTANT

def test_scope_error_remake_level_strict():
    CONSTANT = 42
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', ScopeWarning)
        @rule(outputs={'out': 'out.txt'})    # strict_scope=None → inherit
        def my_rule(outputs):
            return CONSTANT
    with pytest.raises(ScopeError):
        Remake(strict_scope=True, rules=[my_rule])   # enforced at registration

def test_signature_contract():
    with pytest.raises(SignatureError):
        @rule(outputs={'out': '{x}.txt'}, matrix={'x': [1]})
        def bad_rule(inputs, outputs, x):    # declares no inputs — must not take it
            pass
```

**Property-based tests** (Hypothesis):

```python
@given(st.lists(st.integers(min_value=0, max_value=10), min_size=1))
def test_task_keys_unique(matrix_values):
    rule = make_rule(matrix={'x': matrix_values})
    tasks = expand_rule(rule)
    keys = [t.key for t in tasks]
    assert len(keys) == len(set(keys))
```

---

## Migration from remake2

remake2 remakefiles can be adapted to remake3 with a script that:

1. Converts `class MyRule(Rule):` to module-level `@rule(...)` form
2. Replaces `rule_matrix` with `matrix=` (dropping it where empty)
3. Replaces `Rule2.rule_inputs = Rule1.rule_outputs` with `inputs=rule1.outputs`
4. Adds explicit `depends_on=[rule1]` where inferred
5. Inserts `rmk = Remake()` at the top and `rmk.rules_from_current_module()`
   at the end of the file

Decision (2026-06-12): the conversion is LLM translation via the Claude
Code skill (`.claude/skills/remake/references/remake2_to_remake3.md`),
not an automated `remake migrate` command — the long tail of class
hierarchies and implicit globals needs judgement, not regex.

---

## What is explicitly out of scope for remake3

- Content-based change detection (hashing file contents) — the DB is the
  source of truth; filesystem checks are opt-in
- A *passive* GUI or web dashboard — out of scope (the SQLite DB is queryable
  directly by external tools). Note (2026-06-23): an *interactive,
  API-backed* web control plane is being reconsidered as a deliberate
  exception — scheduled as a 0.12.x exploration; see
  [roadmap.md](roadmap.md) and [discussion.md](discussion.md).
- Distributed state beyond what SLURM provides natively
