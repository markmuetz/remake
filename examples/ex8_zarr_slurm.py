# examples/ex8_zarr_slurm.py
#
# Full climate pipeline using:
#   - ZarrStore output tokens (non-file outputs)
#   - Callable inputs with model-specific logic
#   - Per-rule SLURM config overrides
#   - SLURM array jobs for the two largest rules (same matrix, 1-to-1)
#   - Fan-in to per-model aggregation, then a single final task
#
# Run locally:   remake run ex4_zarr_slurm.py
# Run on SLURM:  remake run ex4_zarr_slurm.py --executor slurm

from pathlib import Path
from remake import Remake, ZarrStore, rule

rmk = Remake(
    config={
        'slurm': {
            'partition': 'standard',
            'time':      '01:00:00',
            'mem':       '4G',
        },
    },
)

MODELS = ['era5', 'cmip6_hist', 'cmip6_rcp85', 'cmip6_ssp585']
YEARS  = list(range(1980, 2024))
VARS   = ['tas', 'pr', 'psl']


# --- callable inputs: ERA5 lives in a different archive layout ---

def extract_inputs(model, year):
    if model == 'era5':
        return {v: f'data/archive/era5/{v}/{year}.nc' for v in VARS}
    else:
        return {v: f'data/archive/cmip6/{model}/{v}/{year}.nc' for v in VARS}


# --- rule 1: extract and rechunk to zarr ---
# Heavy I/O — more memory, longer time.

@rule(
    inputs  = extract_inputs,
    outputs = {v: ZarrStore(f'data/zarr/extracted/{{model}}/{{year}}/{v}.zarr') for v in VARS},
    matrix  = {'model': MODELS, 'year': YEARS},
    uses    = {'VARS': VARS},
    config  = {'slurm': {'mem': '32G', 'time': '02:00:00'}},
)
def extract(inputs, outputs, model, year):
    import xarray as xr
    for var in VARS:
        ds = xr.open_dataset(inputs[var])
        # ZarrStore tokens are path-like — pass them straight to zarr/xarray
        ds.chunk({'time': 365}).to_zarr(outputs[var], mode='w')
        import zarr; zarr.consolidate_metadata(outputs[var])


# --- rule 2: compute anomalies relative to a reference period ---
# Same matrix as extract → eligible for SLURM aftercorr array dependency.

REFERENCE_PERIOD = (1981, 2010)    # tracked via uses — change triggers reruns

@rule(
    inputs     = {v: f'data/zarr/extracted/{{model}}/{{year}}/{v}.zarr' for v in VARS},
    outputs    = {v: ZarrStore(f'data/zarr/anomalies/{{model}}/{{year}}/{v}.zarr') for v in VARS},
    matrix     = extract.matrix,
    depends_on = [extract],
    uses       = {'reference_period': REFERENCE_PERIOD, 'VARS': VARS},
    config     = {'slurm': {'mem': '16G', 'time': '01:00:00'}},
)
def anomalies(inputs, outputs, model, year):
    import xarray as xr
    import numpy as np
    ref_start, ref_end = reference_period
    for var in VARS:
        ds   = xr.open_zarr(inputs[var])
        ref  = ds.sel(time=slice(str(ref_start), str(ref_end))).mean('time')
        anom = ds - ref
        anom.chunk({'time': 365}).to_zarr(outputs[var], mode='w')
        import zarr; zarr.consolidate_metadata(outputs[var])


# --- rule 3: aggregate across years for each model ---
# Fan-in: N_years → 1 per model. Falls back to individual SLURM jobs
# (not array) because the matrix collapses here.

def agg_inputs(model):
    return {
        f'{var}_{year}': f'data/zarr/anomalies/{model}/{year}/{var}.zarr'
        for year in YEARS
        for var in VARS
    }


@rule(
    inputs     = agg_inputs,
    outputs    = {v: ZarrStore(f'data/zarr/aggregated/{{model}}/{v}.zarr') for v in VARS},
    matrix     = {'model': MODELS},
    depends_on = [anomalies],
    uses       = {'VARS': VARS, 'YEARS': YEARS},
    config     = {'slurm': {'mem': '64G', 'time': '04:00:00'}},
)
def aggregate(inputs, outputs, model):
    import xarray as xr
    import zarr
    for var in VARS:
        paths = [inputs[f'{var}_{year}'] for year in YEARS]
        ds = xr.open_mfdataset(paths, engine='zarr', combine='by_coords')
        ds.chunk({'time': -1, 'lat': 90, 'lon': 90}).to_zarr(outputs[var], mode='w')
        zarr.consolidate_metadata(outputs[var])


# --- rule 4: single final report across all models ---

@rule(
    inputs     = {
        f'{model}_{var}': f'data/zarr/aggregated/{model}/{var}.zarr'
        for model in MODELS
        for var in VARS
    },
    outputs    = {'report': 'data/results/model_comparison.json'},
    depends_on = [aggregate],
    uses       = {'MODELS': MODELS, 'VARS': VARS},
)
def final_report(inputs, outputs):
    import xarray as xr, json
    summary = {}
    for model in MODELS:
        summary[model] = {}
        for var in VARS:
            ds = xr.open_zarr(inputs[f'{model}_{var}'])
            summary[model][var] = {
                'mean': float(ds[var].mean()),
                'std':  float(ds[var].std()),
            }
    Path(outputs['report']).write_text(json.dumps(summary, indent=2))


rmk.rules_from_current_module()
