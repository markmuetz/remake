# remake examples

Each example is a self-contained pipeline demonstrating part of the remake
API, in roughly increasing order of complexity:

| Example | Demonstrates |
|---|---|
| `ex1_simple.py` | Two chained rules, no matrix |
| `ex2_matrix.py` | Matrix expansion, fan-in, final report |
| `ex3_uses_scope.py` | `uses` tracking of constants and helper functions |
| `ex4_callable_inputs.py` | Input-spec styles (dict, format-string, callable), matrix inheritance/subsetting |
| `ex5_multifile/` | Rules split across modules, combined in a top-level pipeline file |
| `ex6_tuple_matrix.py` | Tuple-key matrices: pre-filtered combo sequences and mixed scalar/tuple axes |
| `ex7_orchestration_only.py` | Orchestration-only pattern: rules with no inputs/outputs |
| `ex8_zarr_slurm.py` | ZarrStore tokens, callable inputs, per-rule SLURM config |
| `ex9_zarr_region.py` | Optional inputs/outputs: source, region-write and pure side-effect rules |
| `ex10_dynamic_matrix.py` | Dynamic matrices (`@deferrable` + `Defer`), non-cartesian `list[dict]` matrices, dynamic fan-in |
| `ex11_custom_token.py` | A custom `OutputToken` (a sqlite table row) and `--check-outputs` verification |
| `ex12_chained_loop_rules.py` | Loop-generated rules with chained dependencies, `name=`, string `depends_on=` |

## Running

remake anchors all paths — a rule's inputs and outputs, *and* the `.remake/`
metadata directory — to the **remakefile's directory**: every command changes
into it first, wherever you invoke from. So copy the examples somewhere
writable, generate the synthetic inputs next to them, and run:

```bash
cp -r /path/to/examples /tmp/remake-examples
cd /tmp/remake-examples
python make_example_data.py      # synthetic inputs (not ex9 — self-generating)
remake run  ex1_simple.py        # .remake/ and data/ live here, next to the file
remake info ex1_simple.py
```

`remake run /tmp/remake-examples/ex1_simple.py` from anywhere else does the
same thing — `.remake/` and `data/` always land next to the remakefile.
(`ex5_multifile/` is its own pipeline directory, so its state lands in there.)

All the examples in this directory therefore share one `.remake/` store —
which is fine: one store per directory is the model, and the rule names are
unique across the whole set. If you define two *different* rules with the
same name in remakefiles sharing a directory, remake warns that they will
clobber each other's recorded state (causing spurious reruns) — that's a
prompt to rename one or split directories, and it's why every rule here has
a distinct name.

ex1, ex3, ex6, ex7, ex10, ex11 and ex12 need only the stdlib. The rest need
the scientific stack (`xarray netCDF4 h5netcdf zarr dask`, plus pyyaml for
ex4) — install everything in one go with the `examples` extra:

```bash
pip install "remake[examples]"     # or: uv add "remake[examples]"
```

## Why heavy imports live inside rule functions

Most examples `import xarray` (or `csv`, `tarfile`, …) *inside* the rule
function body rather than at the top of the file. This is deliberate:

- **Loading a remakefile stays cheap.** remake imports your pipeline file to
  plan the DAG and to answer `info`, `why`, `ls-tasks`, `run --dry-run`, and
  to submit SLURM jobs — none of which execute your tasks. Keeping
  `import xarray` in the body means that whole class of commands doesn't pay
  the (often multi-second) cost of importing heavy scientific libraries.
- **You can inspect a pipeline without every runtime dependency installed.**
  A login node, a CI job, or a colleague can load, plan and introspect the
  pipeline even when `xarray`/`netCDF4`/`zarr` aren't present — those are only
  needed on the machine that actually runs the task. (Contrast
  `ex9_zarr_region.py`, which imports xarray at *module* level because it
  builds coordinates at definition time — so, unlike the others, it can't even
  be loaded without xarray installed.)
- **Tasks often run in a fresh process anyway.** `multiproc` workers reload
  the remakefile and SLURM array elements are brand-new processes; a
  body-local import is the natural idiom there.

Rule of thumb: import inside the function unless the module genuinely needs the
library at definition time (e.g. building a matrix from a `pandas` date range).

## Seeing reruns

Smart rerunning is the point of remake. After running ex3 once:

```bash
remake run ex3_uses_scope.py     # runs everything once
remake run ex3_uses_scope.py     # nothing to do
```

now edit `THRESHOLD` (or the body of `normalise()`) in `ex3_uses_scope.py`
and run again: only `filter_data` and its downstream `combine` rerun —
both are tracked via `uses`. A cosmetic edit (comment, whitespace) reruns
nothing: code is compared by its *structure* (its abstract syntax tree, or
AST), so reformatting never counts as a change. `remake run --dry-run` shows
what would rerun without running it.

Similarly for outputs that vanish behind remake's back (scratch purges):
delete a row that ex11 wrote (`sqlite3 data/results.db "DELETE FROM stats
WHERE key='n2'"`) and rerun with `--check-outputs` — only that task
recomputes.
