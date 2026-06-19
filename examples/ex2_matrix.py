# examples/ex2_matrix.py
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
from remake import Remake, rule

rmk = Remake()

SITES = ['oxford', 'cambridge', 'bristol']
YEARS = list(range(2010, 2021))


@rule(
    inputs  = {'archive': 'data/raw/{site}/{year}.tar.gz'},
    outputs = {'nc': 'data/extracted/{site}/{year}.nc'},
    matrix  = {'site': SITES, 'year': YEARS},
)
def extract(inputs, outputs, site, year):
    import tarfile
    with tarfile.open(inputs['archive']) as tf:
        tf.extractall(Path(outputs['nc']).parent)


@rule(
    inputs     = extract.outputs,
    outputs    = {'processed': 'data/processed/{site}/{year}.nc'},
    matrix     = extract.matrix,
    depends_on = [extract],
)
def process(inputs, outputs, site, year):
    # Heavy imports go inside the rule body so loading/planning the pipeline
    # stays cheap — see "Why heavy imports live inside rule functions" in the
    # examples README.
    import xarray as xr
    ds = xr.open_dataset(inputs['nc'])
    ds.resample(time='1D').mean().to_netcdf(outputs['processed'])


def aggregate_inputs(site):
    """Fan-in: collect all years for this site.

    This function will be called once for each site, and will produce
    an input for each year at that site."""
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


@rule(
    inputs     = {site: f'data/aggregated/{site}.nc' for site in SITES},
    outputs    = {'report': 'data/results/report.html'},
    depends_on = [aggregate],
)
def report(inputs, outputs):
    import xarray as xr
    rows = []
    for site, path in inputs.items():
        ds = xr.open_dataset(path)
        rows.append(f'<tr><td>{site}</td><td>{float(ds.temp.mean()):.2f}</td></tr>')
    html = '<table>' + ''.join(rows) + '</table>'
    Path(outputs['report']).write_text(html)


rmk.rules_from_current_module()
