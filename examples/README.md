# remake examples

Each example is a self-contained pipeline demonstrating part of the remake
API, in roughly increasing order of complexity:

| Example | Demonstrates |
|---|---|
| `ex1_simple.py` | Two chained rules, no matrix |
| `ex2_matrix.py` | Matrix expansion, fan-in, final report |
| `ex3_uses_scope.py` | `uses` tracking of constants and helper functions |
| `ex4_zarr_slurm.py` | ZarrStore tokens, callable inputs, per-rule SLURM config |
| `ex5_callable_inputs_matrix.py` | All input-spec styles, matrix subsetting |
| `ex6_zarr_region.py` | Optional inputs/outputs: source, region-write and pure side-effect rules |
| `ex7_multifile/` | Rules split across modules, combined in a top-level pipeline file |

## Running

From any scratch directory (examples read and write paths relative to the
current directory):

```bash
python /path/to/examples/make_example_data.py   # synthetic inputs (not ex6 — self-generating)
remake run /path/to/examples/ex1_simple.py
remake info /path/to/examples/ex1_simple.py
```

ex1, ex3 and ex5 need only the stdlib (+ pyyaml for ex5). ex2, ex4, ex6 and
ex7 additionally need `xarray netCDF4 h5netcdf zarr dask` (zarr v2 for older
xarray versions).
