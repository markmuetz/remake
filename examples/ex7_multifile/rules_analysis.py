# remake3_examples/ex7_multifile/rules_analysis.py
#
# Cross-file rule wiring: inputs=extract.outputs, matrix=extract.matrix
# and depends_on=[extract] are ordinary attribute accesses on the
# imported Rule object — no shared Remake instance needed.

from pathlib import Path

from remake3 import rule

from rules_extract import extract, SITES, YEARS


@rule(
    inputs     = extract.outputs,
    outputs    = {'processed': 'data/processed/{site}/{year}.nc'},
    matrix     = extract.matrix,
    depends_on = [extract],
)
def process(inputs, outputs, site, year):
    import xarray as xr
    ds = xr.open_dataset(inputs['nc'])
    ds.resample(time='1D').mean().to_netcdf(outputs['processed'])


def aggregate_inputs(site):
    return {str(year): f'data/processed/{site}/{year}.nc' for year in YEARS}


@rule(
    inputs     = aggregate_inputs,
    outputs    = {'agg': 'data/aggregated/{site}.nc'},
    matrix     = {'site': SITES},
    depends_on = [process],
)
def aggregate(inputs, outputs, site):
    import xarray as xr
    ds = xr.open_mfdataset(list(inputs.values()))
    ds.to_netcdf(outputs['agg'])
