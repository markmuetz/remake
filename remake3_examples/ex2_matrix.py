# remake3_examples/ex2_matrix.py
#
# Matrix expansion: process multiple sites and years independently,
# then aggregate per-site across years.
#
# DAG:
#   extract[site, year]  (N_sites x N_years tasks)
#       |
#   process[site, year]  (N_sites x N_years tasks)
#       |
#   aggregate[site]      (N_sites tasks — fan-in across years)
#       |
#   report               (1 task — fan-in across sites)

from pathlib import Path
from remake3 import Remake

rmk = Remake()

SITES = ['oxford', 'cambridge', 'bristol']
YEARS = list(range(2010, 2021))


@rmk.rule(
    inputs  = {'archive': 'data/raw/{site}/{year}.tar.gz'},
    outputs = {'nc': 'data/extracted/{site}/{year}.nc'},
    matrix  = {'site': SITES, 'year': YEARS},
)
def extract(inputs, outputs, site, year):
    import tarfile
    Path(outputs['nc']).parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(inputs['archive']) as tf:
        tf.extractall(Path(outputs['nc']).parent)


@rmk.rule(
    inputs     = extract.outputs,
    outputs    = {'processed': 'data/processed/{site}/{year}.nc'},
    matrix     = extract.matrix,
    depends_on = [extract],
)
def process(inputs, outputs, site, year):
    import xarray as xr
    Path(outputs['processed']).parent.mkdir(parents=True, exist_ok=True)
    ds = xr.open_dataset(inputs['nc'])
    ds.resample(time='1D').mean().to_netcdf(outputs['processed'])


def aggregate_inputs(site):
    """Fan-in: collect all years for this site."""
    return {str(year): f'data/processed/{site}/{year}.nc' for year in YEARS}


@rmk.rule(
    inputs     = aggregate_inputs,
    outputs    = {'agg': 'data/aggregated/{site}.nc'},
    matrix     = {'site': SITES},
    depends_on = [process],
)
def aggregate(inputs, outputs, site):
    import xarray as xr
    Path(outputs['agg']).parent.mkdir(parents=True, exist_ok=True)
    ds = xr.open_mfdataset(list(inputs.values()))
    ds.to_netcdf(outputs['agg'])


@rmk.rule(
    inputs     = {site: f'data/aggregated/{site}.nc' for site in SITES},
    outputs    = {'report': 'data/results/report.html'},
    matrix     = {},
    depends_on = [aggregate],
)
def report(inputs, outputs):
    import xarray as xr
    Path(outputs['report']).parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for site, path in inputs.items():
        ds = xr.open_dataset(path)
        rows.append(f'<tr><td>{site}</td><td>{float(ds.temp.mean()):.2f}</td></tr>')
    html = '<table>' + ''.join(rows) + '</table>'
    Path(outputs['report']).write_text(html)
