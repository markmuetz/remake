# remake3 — Detailed Code Implementation Plan

How to transform the existing remake2 code in `src/remake/` into the design in
[remake3_design.md](remake3_design.md). Covers the next five items of the
[implementation plan](remake3_implementation_plan.md): core, output tokens,
metadata backend, executors, dynamic matrices.

Guiding choice throughout: **simple over complex**. Where remake2 had a
feature the design doc doesn't require, it goes (it stays in git history and
can come back later). The goal is a clean core of small, mostly-pure modules.

Out of scope here (separate plan items): CLI rewrite, tests, docs, CI.
`remake_cmd.py` and `util/command_line_args.py` are untouched for now even
though their handlers will break against the new API — they are rewritten in
the CLI item.

---

## 1. Core (`src/remake/core/`)

The biggest change. remake2's model — class-based rules, DAG wired by matching
output path strings to input path strings, all tasks materialised at load
time — is replaced wholesale by decorator-defined rules, explicit
`depends_on`, and lazy task expansion. Most of `core/` is therefore new code;
the old orchestration hub (`remake.py`, 475 lines) does not survive
adaptation and is rewritten.

### New `rule.py` — replaces class-based `Rule`

- Module-level `rule(...)` decorator returning a `Rule` dataclass (fields per
  the design doc: `fn`, `inputs`, `outputs`, `matrix`, `depends_on`, `uses`,
  `strict_scope`, `config`, `remake`).
- Decoration time does exactly two things: validate the signature contract
  (`inspect.signature`: function params must be `[inputs,] [outputs,]
  <matrix keys>`; `inputs={}`/`outputs={}` rejected; raises `SignatureError`)
  and run scope analysis (warnings, or `ScopeError` if rule-level
  `strict_scope=True`).
- **What goes from old `rule.py`:** the `Rule` base class, `run_task()`
  classmethod, and the atomic tmp-path output writes (`.remake.tmp.X`
  renaming). Atomic writes are a real feature but add machinery to every
  execution path; dropped for now, candidate to add back behind a config
  flag later. Parent-directory creation survives but moves to the
  task-execution wrapper (section 2).

### New `scope.py` — replaces `util/decorators.py`

- Free-variable *detection*, not runtime isolation: inspect
  `fn.__code__.co_names` + `co_freevars`, filter against builtins, stdlib
  module names, and names declared in `uses`; emit `ScopeWarning` or raise
  `ScopeError`.
- `uses_hash(uses)` helper: `repr()` for plain values, source-AST text for
  functions — the string stored in the DB and compared by the planner.
- **`util/decorators.py` is deleted.** Its `rule_dec()` attempted runtime
  scope *isolation* (clearing globals around the call) and is incomplete
  (bare `raise`). Detection + `uses` tracking is the design; isolation is
  complexity we don't need.

### New `task.py` — replaces eager `Task`

- Lazy `Task` dataclass per the design: identity is `(rule, kwargs)`;
  `key = sha1(f'{rule.fn.__name__}:{kwargs!r}')`; `inputs`/`outputs` are
  `cached_property`, resolved from the rule's spec (dict `.format(**kwargs)`
  interpolation, or callable called with `**kwargs`) on first access.
- **What goes:** `prev_tasks`/`next_tasks` pointers (no task-level DAG at
  all — see below), the old key definition (sha1 of input+output paths;
  the new key must not require resolving paths), `diff()` (moves to
  CLI/reporting later), and DB-state fields living on the task
  (`last_run_status` etc. — the planner reads those from the metadata
  backend instead; `Task` stays a pure value object).

### New `dag.py` — replaces path-matching DAG construction

- Three pure functions, as in the design doc: `build_rule_dag(rules)`
  (networkx `DiGraph` from `depends_on`, acyclicity assert),
  `expand_rule(rule)`, and `_resolve_matrix(matrix)` (normalise
  `None` / `{key: [values]}` / `list[dict]` / callable to canonical
  `list[dict]`; callables may raise `MatrixNotReady`).
- **What goes:** all of remake2's task-graph construction — the
  output-path→input-path matching in `Remake.load_rules()`, the
  `task_dag`, and the inferred rule-level graph. There is **no task-level
  DAG in remake3 at all**: execution order is rule DAG order, and
  within-wave tasks are independent by construction. This is the single
  biggest simplification and is what makes 1e6-task pipelines loadable.
  Known limitation (intra-rule task dependencies are inexpressible):
  see "No task-level DAG" in [remake3_design.md](remake3_design.md).

### New `planner.py` — subsumes and deletes `task_control.py`

- Pure `plan(rules, dag, metadata, query=None, force=False)` →
  `(runnable_tasks, deferred_rules)`, per the design doc.
- Rerun logic carried over from `task_control.py` in simplified, DB-first
  form: never run → rerun; failed → rerun; rule_run AST changed (existing
  `CodeComparer`) → rerun; `uses_hash` changed → rerun; any upstream task
  reruns → rerun.
- **What goes:** all mtime comparison and input/output file-existence
  checking from `task_control.py`. File checks are the planner's opt-in
  `check_outputs` hook (section 2), not core rerun logic. `task_control.py`
  is deleted once its logic is ported.

### Rewritten `remake.py`

A much smaller `Remake` class:

- Construction: `config`, injected `metadata` backend (default
  `Sqlite3Backend('.remake/remake.db')`), `check_outputs`, `strict_scope`,
  optional `rules=[...]`.
- Registration: `add_rules()` (dedupe by identity, resolve tri-state
  `strict_scope`/config against Remake defaults, set `rule.remake`),
  `rules_from_current_module()` (caller-frame globals scan),
  `rules_from_modules(*mods)`.
- `finalize()`: build rule DAG, sync rule/task metadata with the backend.
- `run(executor=...)`: the replanning loop (section 5) around
  `plan()` + `executor.run_tasks()`.
- `run_task(key)`: resolve a key to a Task and execute it — the single
  entry point used by multiproc workers and SLURM jobs.
- The task-execution wrapper (used by `run_task` and the singleproc path):
  resolve inputs/outputs, create parent dirs for path-backed outputs, call
  `rule.fn`, record success/failure in metadata.
- **What goes from old `remake.py`:** `load_rules`/`autoload_rules`/
  `load_rule` (path wiring, eager expansion, `var_matrix` legacy handling),
  `pyquerylist` task lists (plain `list` now; querying is a CLI concern),
  `tabulate` reporting (CLI item), `touch` (meaningless without
  mtime-based logic), and the `task_key_map` of all materialised tasks
  (lazy: keys are resolved through rule expansion on demand).

### `loader/`, `exceptions.py`, `util/`

- `loader/__init__.py`: keep the generic `load_module()`; rewrite
  `load_remake()` to its minimal job — import the pipeline file, find the
  single `Remake` instance in its namespace, return it. `load_archive()`
  goes (with `archive.py`).
- `core/exceptions.py`: keep; add `MatrixNotReady` (carries blocking path
  strings), `ScopeError`, `SignatureError`. `ScopeWarning` lives in
  `scope.py`.
- `util/code_compare.py`: **keep unchanged** — pure, self-contained, used
  by the planner and metadata backend.
- `util/config.py`: keep (Remake-level + per-rule config merging).
- `util/util.py`: trim — keep `sysrun`, `format_path`, `Capturing`; drop
  `get_git_info`/`git_archive` (archive-only) and `tmp_to_actual_path`
  (atomic writes dropped).
- **`core/archive.py` is deleted.** Archiving outputs+git state to tar is
  a remake2 feature with no place in the remake3 design; it can be
  resurrected from git history as a post-1.0 feature if wanted.

### Task filtering — pyquerylist removal confirmed

remake2 filters tasks by materialising every Task, splatting its kwargs
onto it as attributes, and running `pyquerylist.where(query)` over the
list. In remake3 a task is `(rule, kwargs)` and nothing else, so filtering
reduces to filtering plain dicts — no library, no attribute splatting, and
no Task construction for filtered-out tasks. Three filter kinds, three
sites:

1. **Rule filters** (`--rule extract`) — applied *before expansion*:
   select `Rule` objects by name. Excluded rules are never expanded.
2. **kwargs filters** (the planner's `query` parameter) — applied *during
   expansion, before Task construction*, as a predicate over the
   `list[dict]` from `_resolve_matrix()`. Inputs/outputs are never
   resolved for filtered-out tasks. The whole query mechanism is:

   ```python
   def make_predicate(query: str):
       """e.g. "year > 1985 and model == 'era5'" """
       code = compile(query, '<query>', 'eval')
       return lambda kwargs: eval(code, {'__builtins__': {}}, kwargs)
   ```

   eval of a user-supplied query is fine — it runs at the same trust
   level as the remakefile itself. An expression query subsumes
   `key=value` syntax (`year == 1980`), so it is the only mechanism.
3. **Status filters** (`--status failed`) — applied *after planning*, from
   one batched `get_tasks_status()` call; per-rule summaries
   (`info --rule`) are a `collections.Counter` over the same records.

Caveat to carry into the planner: a filtered run (`year == 1980`) must not
let downstream propagation assume the other years ran. DB-first status
handles this naturally — unrun tasks simply remain unrun and reappear in
the next plan — but tests should pin it.

### Keep / adapt / delete — core

| File | Action |
|---|---|
| `core/remake.py` | Rewrite (small `Remake`) |
| `core/rule.py` | Replace (decorator + `Rule` dataclass) |
| `core/task.py` | Replace (lazy value object) |
| `core/task_control.py` | Delete (logic → `planner.py`) |
| `core/archive.py` | Delete |
| `core/exceptions.py` | Keep, extend |
| `core/dag.py`, `core/planner.py`, `core/scope.py` | New |
| `loader/__init__.py` | Adapt (keep `load_module`, minimal `load_remake`) |
| `util/code_compare.py` | Keep |
| `util/config.py` | Keep |
| `util/util.py` | Trim |
| `util/decorators.py` | Delete |

---

## 2. Output tokens

New code; remake2 has no equivalent (outputs are bare `Path`s).

- **One module, `src/remake/tokens.py`** — not a `tokens/` package. Four
  small classes don't need five files; split later if token types
  proliferate.
- Contents per the design doc: `OutputToken` ABC (`identity()`,
  `is_complete()`, `__str__`), transparent `PathToken` base (`__fspath__`),
  `FileToken`, `ZarrStore`, `S3Object`. Plus `as_token(value)`: wraps plain
  strings in `FileToken`, passes tokens through — called once, at
  spec-resolution time in `Task.outputs`.
- Parent-directory creation: in the task-execution wrapper in `remake.py`,
  `os.fspath()`-able tokens get `Path(token).parent.mkdir(parents=True,
  exist_ok=True)` before `rule.fn` is called. Output-less rules: nothing
  to do.
- `check_outputs` (`Remake` arg + planner parameter): `'never'`,
  `'fallback'` (default — consult `is_complete()` only for tasks with no
  DB record), `'always'` (consult for every planned task; detects purged
  outputs). First implementation ships `never` + `fallback`; `always` is a
  small extension of the same planner hook and can follow immediately.
  Output-less tasks are DB-authoritative in all modes.

---

## 3. Metadata backend (`src/remake/metadata/`)

The part of remake2 that survives best. The schema, the EXCLUSIVE-lock
transactions, and the exponential-backoff retry decorator in
`sqlite3_metadata_manager.py` are kept.

- `metadata_manager.py`: narrow the ABC to the design's three methods —
  `get_or_create_task(task) -> TaskRecord`, `update_task(task,
  exception='')`, `get_tasks_status(tasks) -> dict[str, TaskRecord]`.
  `TaskRecord` is a small frozen dataclass (status, timestamp, run_code,
  uses_hash, exception) — the planner consumes these instead of mutating
  Task fields.
- `sqlite3_metadata_manager.py` (rename class to `Sqlite3Backend` per the
  design doc): adapt rather than rewrite.
  - Schema changes: add `uses_hash TEXT` to `task`; `task.key` now comes
    from the new `(rule_name, kwargs)` key. `code` and `rule` tables
    (inputs/outputs/run code references) stay as-is.
  - Port `get_or_create_rule_metadata` / `get_or_create_tasks_metadata` to
    the new `Rule` (source captured at decoration via `inspect.getsource`)
    and `Task` shapes.
  - No schema migration support: pre-release, a changed schema means
    delete `.remake/remake.db` and rerun. Document this.
  - `:memory:` already works with sqlite3; keep it working — it is the
    test substrate.
- **What goes:** any rule/task metadata writing driven from the old
  class-based `Rule.source` dict; replaced by source capture on the new
  `Rule` dataclass.

| File | Action |
|---|---|
| `metadata/metadata_manager.py` | Adapt (narrow ABC, add `TaskRecord`) |
| `metadata/sqlite3_metadata_manager.py` | Adapt (schema tweak, new Rule/Task shapes) |

---

## 4. Executors (`src/remake/executors/`)

- **`executor.py`**: keep the ABC; signature simplifies to
  `run_tasks(tasks: list[Task])`. The reporting flags (`show_reasons`,
  `show_task_code_diff`, `stdout_to_log`) move to the CLI item.
- **`singleproc_executor.py`**: keep — it stays a dumb loop calling the
  task-execution wrapper. The dynamic-matrix replanning loop lives in
  `Remake.run`, *not* in executors, so singleproc/multiproc get deferral
  handling for free.
- **`multiproc_executor.py`**: adapt. The existing design — workers reload
  the remakefile via the loader and execute tasks fetched by key from a
  queue — is sound and survives; port it to the new `load_remake()` +
  `Remake.run_task(key)`. What goes: `task_key_map` dependence (workers
  ask the Remake for the task by key; `Remake.run_task` does the lookup).
- **`slurm_executor.py`**: rewrite to the design doc. The old model —
  one sbatch job per task, job identity via 10-char key in the job name,
  `squeue` scraped by name — goes entirely. The new model:
  1. per-rule JSON job specs `.remake/jobs/<rule>.json` (array index =
     index into the array) + `<rule>.jobids.json` sidecars;
  2. one `.sbatch` script per rule (array-parameterised where eligible)
     + master `.remake/submit.sh` with `sbatch --parsable` and
     `--dependency=aftercorr/afterok` wiring;
  3. array eligibility: no intra-rule deps ∧ same matrix as upstream ∧
     size ≥ threshold (default 10);
  4. submission = execute `submit.sh`; `--dry-run` stops before that;
     `resubmit` re-executes it; already-queued detection reads the jobid
     sidecar and checks `squeue` by job ID + array index.
  What survives from the old file: per-rule SLURM config merged over
  Remake defaults, and the general sbatch-template approach.
- **`dask_executor.py`: delete.** It is incomplete (unfinished config
  handling, typos) and dask is last in priority. Re-add against the new
  ABC when its turn comes; until then the ABC stays honest with three real
  implementations.

| File | Action |
|---|---|
| `executors/executor.py` | Keep (signature update) |
| `executors/singleproc_executor.py` | Keep |
| `executors/multiproc_executor.py` | Adapt |
| `executors/slurm_executor.py` | Rewrite |
| `executors/dask_executor.py` | Delete (re-add later) |

---

## 5. Dynamic matrices

New behaviour; nothing in remake2 to remove. Lands in two steps:

1. **Local (with core):** callable matrices in `dag._resolve_matrix`
   raising `MatrixNotReady`; `plan()` catches it and returns the rule in
   `deferred_rules`; `Remake.run` drives the loop from the design doc —
   run a wave, retry deferred rules, stop reporting *blocked* when a rule
   stays deferred although all its `depends_on` tasks completed. Because
   the loop is in `Remake.run`, it works identically for singleproc and
   multiproc.
2. **SLURM (with the SLURM rewrite):** rules whose matrix raised
   `MatrixNotReady` are excluded from `submit.sh`; a lightweight
   `continuation.sbatch` (re-invoking `remake run --executor slurm`,
   `--dependency=afterok:` the last submitted wave) is appended whenever
   deferred rules remain. Idempotent replanning makes arbitrarily deep
   chains work without extra machinery. Task-key stability over kwargs
   (not paths) is what makes re-entry find existing DB records.

---

## Order of work

Sections land roughly in this order, each with its unit tests (testing
itself is a separate plan item; this is just sequencing):

1. `exceptions`, `scope`, `rule` (decorator + contract) — no dependencies.
2. `tokens`, `task`, `dag` — pure, independently testable.
3. `metadata` adaptation, then `planner` (needs `TaskRecord`).
4. `remake.py` + `loader` + singleproc → first end-to-end pipeline.
5. Multiproc, dynamic matrices step 1.
6. SLURM rewrite + continuation jobs (largest single chunk).

Deletions (`archive.py`, `task_control.py`, `decorators.py`,
`dask_executor.py`, old `rule.py`/`task.py` content) happen as their
replacements land, not up front — the package stays importable throughout.

## Dependency changes

- Drop: `pyquerylist` (plain lists), `tabulate` (revisit at the CLI item).
- Keep: `loguru`, `networkx`.
- No new runtime dependencies. `boto3` is imported lazily inside
  `S3Object.is_complete()` only; not a declared dependency.

## Full file disposition

| File | Action |
|---|---|
| `__init__.py` | Rewrite exports (`Remake`, `rule`, tokens, exceptions) |
| `version.py` | Keep |
| `remake_cmd.py` | Untouched here (CLI item) |
| `core/remake.py` | Rewrite |
| `core/rule.py` | Replace |
| `core/task.py` | Replace |
| `core/task_control.py` | Delete |
| `core/archive.py` | Delete |
| `core/exceptions.py` | Keep, extend |
| `core/dag.py` / `core/planner.py` / `core/scope.py` | New |
| `tokens.py` | New |
| `metadata/metadata_manager.py` | Adapt |
| `metadata/sqlite3_metadata_manager.py` | Adapt |
| `executors/executor.py` | Keep |
| `executors/singleproc_executor.py` | Keep |
| `executors/multiproc_executor.py` | Adapt |
| `executors/slurm_executor.py` | Rewrite |
| `executors/dask_executor.py` | Delete |
| `loader/__init__.py` | Adapt |
| `util/code_compare.py` | Keep |
| `util/config.py` | Keep |
| `util/util.py` | Trim |
| `util/command_line_args.py` | Untouched here (CLI item) |
| `util/decorators.py` | Delete |
| `examples/ex1.py` | Delete (superseded by top-level `examples/`) |
