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
  `@rmk.rule(...)`, not classes used as namespaces.
- **Explicit dependencies** — the rule-level DAG is declared via `depends_on`,
  not inferred from path strings.
- **Lazy task expansion** — the rule-level DAG is built eagerly (it is small);
  individual `Task` objects are expanded on demand.
- **Pluggable output tokens** — outputs are token objects (`FileToken`,
  `ZarrStore`, `S3Object`, ...) with a common `is_complete()` interface.
- **Dependency injection throughout** — metadata backend, executor, and
  filesystem access are all injected, never hardwired.
- **Controlled scope** — external dependencies of `rule_run` must be declared
  in `uses`; undeclared free variables produce warnings (or errors in strict
  mode).
- **Testability as a first-class concern** — every subsystem is independently
  unit-testable; integration tests use `tmp_path` and in-memory SQLite.

---

## API overview

### Defining a pipeline

```python
# climate_pipeline.py
from pathlib import Path
from remake3 import Remake, ZarrStore

rmk = Remake()

MODELS = ['era5', 'cmip6_hist', 'cmip6_rcp85']
YEARS  = list(range(1980, 2021))

# --- rule 1: extract ---

def extract_inputs(model, year):
    """Callable inputs — full Python, no restrictions."""
    base = 'data/raw/era5' if model == 'era5' else f'data/raw/{model}'
    return {'raw': f'{base}/{year}.nc'}

@rmk.rule(
    inputs  = extract_inputs,
    outputs = {'clean': 'data/clean/{model}/{year}.nc'},
    matrix  = {'model': MODELS, 'year': YEARS},
)
def extract(inputs, outputs, model, year):
    import xarray as xr
    ds = xr.open_dataset(inputs['raw'])
    ds.to_netcdf(outputs['clean'])


# --- rule 2: compute anomalies (depends on extract) ---

@rmk.rule(
    inputs  = extract.outputs,          # rule object carries its output spec
    outputs = {'zarr': ZarrStore('data/anomalies/{model}/{year}.zarr')},
    matrix  = extract.matrix,           # inherit matrix
    depends_on = [extract],
)
def anomalies(inputs, outputs, model, year):
    import xarray as xr
    ds = xr.open_dataset(inputs['clean'])
    clim = ds.mean('time')
    (ds - clim).to_zarr(outputs['zarr'].path)


# --- rule 3: aggregate all years per model (fan-in) ---

def agg_inputs(model):
    return {str(year): f'data/anomalies/{model}/{year}.zarr' for year in YEARS}

@rmk.rule(
    inputs     = agg_inputs,
    outputs    = {'agg': ZarrStore('data/aggregated/{model}.zarr')},
    matrix     = {'model': MODELS},
    depends_on = [anomalies],
)
def aggregate(inputs, outputs, model):
    import xarray as xr
    ds = xr.open_mfdataset(list(inputs.values()), engine='zarr')
    ds.to_zarr(outputs['agg'].path)
```

### Using `uses` for tracked external dependencies

```python
THRESHOLD = 0.5                          # module-level constant

@rmk.rule(
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
identical. Functions in `uses` are compared by AST; plain values by `repr()` or
a user-supplied hash function.

### Strict scope mode

```python
rmk = Remake(strict_scope=True)
# or per-rule:
@rmk.rule(..., strict_scope=True)
def my_rule(inputs, outputs, year): ...
```

In strict mode, if `rule_run` accesses any name from outer scope that is not
covered by `uses`, decoration raises `ScopeError`. In the default mode
(`strict_scope=False`) this is a warning.

Detection uses `fn.__code__.co_names` (globals accessed) and
`fn.__code__.co_freevars` (closure variables), filtered against a known-safe
list (stdlib modules, builtins).

---

## Architecture

```
remake3/
├── core/
│   ├── remake.py          # Remake class — wires everything together
│   ├── rule.py            # Rule descriptor (produced by @rmk.rule)
│   ├── task.py            # Task dataclass — lightweight, lazy
│   ├── dag.py             # Rule-level DAG construction (pure functions)
│   ├── planner.py         # Decides which tasks need running (pure)
│   └── scope.py           # Free-variable analysis, uses tracking
├── tokens/
│   ├── base.py            # OutputToken ABC
│   ├── file_token.py      # FileToken — wraps a path string
│   ├── zarr_token.py      # ZarrStore — checks consolidated metadata
│   └── s3_token.py        # S3Object — checks object existence via boto3
├── metadata/
│   ├── base.py            # MetadataManager ABC
│   └── sqlite3_backend.py # Production backend
├── executors/
│   ├── base.py            # Executor ABC
│   ├── singleproc.py
│   ├── multiproc.py
│   ├── slurm.py           # Submits array jobs where possible
│   └── dask.py
├── util/
│   ├── code_compare.py    # AST-based code comparison (carry over)
│   └── matrix.py          # Cartesian expansion helpers
└── cli.py                 # Entry point — thin wrapper over Remake
```

### The `Rule` descriptor

`@rmk.rule(...)` returns a `Rule` object, not the original function. This
object carries:

```python
@dataclass
class Rule:
    fn:         Callable                     # the rule_run function
    inputs:     dict | Callable              # spec or callable → spec
    outputs:    dict | Callable              # spec or callable → spec
    matrix:     dict                         # {param: [values], ...}
    depends_on: list[Rule]                   # explicit upstream rules
    uses:       dict                         # tracked external names/values
    strict_scope: bool
    config:     dict                         # executor overrides (e.g. slurm mem)
    # set by Remake after registration:
    remake:     Remake | None = None
```

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
    """Expand the matrix for one rule into Task objects (no I/O)."""
    combos = list(itertools.product(*rule.matrix.values()))
    keys   = list(rule.matrix.keys())
    return [Task(rule=rule, kwargs=dict(zip(keys, combo))) for combo in combos]
```

Because these are pure functions they are trivially unit-testable.

### The planner (pure)

```python
def plan(
    rules:    list[Rule],
    dag:      nx.DiGraph,
    metadata: MetadataManager,
    query:    str | None = None,
    force:    bool = False,
) -> list[Task]:
    """
    Return the ordered list of tasks that need running.
    Pure except for metadata reads (injected).
    """
```

The planner reads task status from the metadata backend and returns a flat
ordered list. It never writes to the DB, never touches the filesystem. The
returned list is passed to an executor.

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
    def get_or_create_task(self, task: Task) -> TaskRecord: ...
    @abstractmethod
    def update_task(self, task: Task, exception: str = '') -> None: ...
    @abstractmethod
    def get_tasks_status(self, tasks: list[Task]) -> dict[str, TaskRecord]: ...
```

---

## SLURM executor

### User-facing interface

From the user's perspective there is one command:

```bash
remake3 run mypipeline.py --executor slurm
```

Internally this is a two-stage process — generate job specs and submit scripts,
then execute the master submit script — but that split is an implementation
detail, not a workflow the user has to manage.

Two additional commands are available:

```bash
remake3 run mypipeline.py --executor slurm --dry-run
# Stage 1+2 only: writes .remake/jobs/ and .remake/submit.sh without submitting.
# Useful for inspecting what would be submitted before touching the cluster.

remake3 resubmit mypipeline.py
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
parameterised by `$SLURM_ARRAY_TASK_ID`; for individual-job rules by the task
key passed at submission time.

**Array script** (rule is array-eligible, see below):

```bash
#!/bin/bash
#SBATCH --job-name=extract
#SBATCH --array=0-40
#SBATCH -o .remake/slurm/output/extract/%a.out
#SBATCH -e .remake/slurm/output/extract/%a.err
{extra_opts}

TASK_KEY=$(python -c "
import json
print(json.load(open('.remake/jobs/extract.json'))[$SLURM_ARRAY_TASK_ID]['task_key'])
")
echo "SLURM RUNNING $TASK_KEY"
remake3 run-task mypipeline.py $TASK_KEY
echo "SLURM COMPLETED $TASK_KEY"
```

The script is intentionally dumb — it reads the JSON array to find its task key
and delegates to `remake3 run-task`. No task-specific information needs to be
embedded in the job name or comment, which sidesteps the array job identification
problem entirely.

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

`remake3 run --executor slurm` executes `.remake/submit.sh` immediately after
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
@rmk.rule(
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

## Output tokens

All outputs are `OutputToken` instances. String paths are automatically wrapped
in `FileToken` for backwards convenience.

```python
class OutputToken(ABC):
    @abstractmethod
    def identity(self) -> str:
        """Stable string used for DAG wiring and task key hashing."""

    @abstractmethod
    def is_complete(self) -> bool:
        """Has this output been successfully produced?"""


class FileToken(OutputToken):
    def __init__(self, path: str):
        self.path = path

    def identity(self) -> str:
        return self.path

    def is_complete(self) -> bool:
        return Path(self.path).exists()


class ZarrStore(OutputToken):
    def __init__(self, path: str):
        self.path = path

    def identity(self) -> str:
        return self.path

    def is_complete(self) -> bool:
        # Consolidated metadata written by zarr.consolidate_metadata()
        return Path(self.path, '.zmetadata').exists()


class S3Object(OutputToken):
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

Token `is_complete()` is only called when filesystem checks are explicitly
enabled in config — the default is DB-first, filesystem as a fallback.

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
    rmk = Remake(metadata=InMemoryBackend())

    @rmk.rule(inputs={}, outputs={'a': 'a.txt'}, matrix={})
    def rule_a(inputs, outputs): pass

    @rmk.rule(inputs=rule_a.outputs, outputs={'b': 'b.txt'},
              matrix={}, depends_on=[rule_a])
    def rule_b(inputs, outputs): pass

    dag = build_rule_dag(rmk.rules)
    assert list(nx.topological_sort(dag)) == [rule_a, rule_b]


def test_matrix_expansion():
    rule = make_rule(matrix={'model': ['a', 'b'], 'year': [2000, 2001]})
    tasks = expand_rule(rule)
    assert len(tasks) == 4
    assert {t.kwargs['model'] for t in tasks} == {'a', 'b'}


def test_planner_reruns_on_code_change(tmp_path):
    meta = Sqlite3Backend(':memory:')
    rmk  = Remake(metadata=meta)

    @rmk.rule(inputs={}, outputs={'out': str(tmp_path / '{x}.txt')}, matrix={'x': [1]})
    def my_rule(inputs, outputs, x):
        pass

    rmk.finalize()
    tasks = plan(rmk.rules, rmk.dag, meta)
    assert tasks[0].requires_rerun  # never run

    # Simulate a completed run
    meta.update_task(tasks[0])

    # Mutate the source
    my_rule.fn = lambda inputs, outputs, x: print('changed')
    tasks = plan(rmk.rules, rmk.dag, meta)
    assert tasks[0].requires_rerun  # code changed
```

**Integration tests** — real filesystem via `tmp_path`, in-memory DB:

```python
@pytest.fixture
def rmk(tmp_path):
    return Remake(
        metadata=Sqlite3Backend(':memory:'),
        root=tmp_path,
    )

def test_two_rule_pipeline(rmk, tmp_path):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'raw' / 'data.txt').write_text('hello')

    @rmk.rule(
        inputs  = {'src': str(tmp_path / 'raw/data.txt')},
        outputs = {'dst': str(tmp_path / 'out/data.txt')},
        matrix  = {},
    )
    def process(inputs, outputs):
        Path(outputs['dst']).parent.mkdir(exist_ok=True)
        Path(outputs['dst']).write_text(Path(inputs['src']).read_text().upper())

    rmk.run()
    assert Path(tmp_path / 'out/data.txt').read_text() == 'HELLO'
```

**Scope warning tests:**

```python
def test_scope_warning_on_free_variable():
    rmk = Remake()
    CONSTANT = 42
    with pytest.warns(ScopeWarning, match='CONSTANT'):
        @rmk.rule(inputs={}, outputs={'out': 'out.txt'}, matrix={})
        def my_rule(inputs, outputs):
            return CONSTANT   # closes over CONSTANT, not declared in uses

def test_scope_error_in_strict_mode():
    rmk = Remake(strict_scope=True)
    CONSTANT = 42
    with pytest.raises(ScopeError):
        @rmk.rule(inputs={}, outputs={'out': 'out.txt'}, matrix={})
        def my_rule(inputs, outputs):
            return CONSTANT
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

1. Converts `class MyRule(Rule):` to `@rmk.rule(...)` form
2. Replaces `rule_matrix` with `matrix=`
3. Replaces `Rule2.rule_inputs = Rule1.rule_outputs` with `inputs=rule1.outputs`
4. Adds explicit `depends_on=[rule1]` where inferred

A `remake3 migrate myfile.py` CLI command will run this conversion.

---

## What is explicitly out of scope for remake3

- Content-based change detection (hashing file contents) — the DB is the
  source of truth; filesystem checks are opt-in
- A GUI or web dashboard — out of scope, but the SQLite DB is queryable
  directly by external tools
- Distributed state beyond what SLURM provides natively
