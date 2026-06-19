# examples/make_example_data.py
#
# Generate synthetic input data for all examples, under the current
# directory. Run from the directory you will run the examples from:
#
#   python make_example_data.py            # all examples
#   python make_example_data.py ex1 ex3    # just some
#
# ex9 generates its own data and needs nothing here.
import io
import random
import sys
import tarfile
from pathlib import Path


def ex1():
    """data/raw/measurements.csv — csv with comment lines."""
    rng = random.Random(1)
    lines = ['# measurements file', '# site: test']
    lines += ['time,value'] + [f'{i},{rng.uniform(0, 10):.2f}' for i in range(20)]
    path = Path('data/raw/measurements.csv')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines) + '\n')


def _temperature_nc_bytes(year, seed):
    """A month of 6-hourly synthetic temperature as netCDF bytes."""
    import tempfile

    import numpy as np
    import pandas as pd
    import xarray as xr

    time = pd.date_range(f'{year}-01-01', f'{year}-01-31', freq='6h')
    rng = np.random.default_rng(seed)
    ds = xr.Dataset(
        {'temp': (('time',), rng.normal(12, 5, len(time)))},
        coords={'time': time},
    )
    with tempfile.NamedTemporaryFile(suffix='.nc') as f:
        ds.to_netcdf(f.name, engine='h5netcdf')
        return Path(f.name).read_bytes()


def ex2():
    """data/raw/{site}/{year}.tar.gz, each containing {year}.nc (also ex5)."""
    sites = ['oxford', 'cambridge', 'bristol']
    years = range(2010, 2021)
    for i, site in enumerate(sites):
        for year in years:
            path = Path(f'data/raw/{site}/{year}.tar.gz')
            path.parent.mkdir(parents=True, exist_ok=True)
            nc_bytes = _temperature_nc_bytes(year, seed=i * 1000 + year)
            with tarfile.open(path, 'w:gz') as tf:
                info = tarfile.TarInfo(f'{year}.nc')
                info.size = len(nc_bytes)
                tf.addfile(info, io.BytesIO(nc_bytes))


def ex3():
    """data/raw/{year}.csv for 2000-2009 with a 'value' column."""
    rng = random.Random(3)
    for year in range(2000, 2010):
        path = Path(f'data/raw/{year}.csv')
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = ['value'] + [f'{rng.uniform(0, 1):.3f}' for _ in range(50)]
        path.write_text('\n'.join(rows) + '\n')


def ex4():
    """config/pipeline.yaml and data/raw/{site}/{year}.csv."""
    Path('config').mkdir(exist_ok=True)
    Path('config/pipeline.yaml').write_text('version: 1\nname: ex4\n')
    rng = random.Random(5)
    for site in ['site_a', 'site_b', 'site_c']:
        for year in [2020, 2021, 2022]:
            path = Path(f'data/raw/{site}/{year}.csv')
            path.parent.mkdir(parents=True, exist_ok=True)
            rows = ['value'] + [f'{rng.uniform(5, 15):.2f}' for _ in range(30)]
            path.write_text('\n'.join(rows) + '\n')


def ex8():
    """data/archive/... — one small monthly netCDF per model/var/year."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    models = ['era5', 'cmip6_hist', 'cmip6_rcp85', 'cmip6_ssp585']
    years = range(1980, 2024)
    variables = ['tas', 'pr', 'psl']
    rng = np.random.default_rng(4)
    lat = np.linspace(-60, 60, 4)
    lon = np.linspace(0, 270, 4)
    for model in models:
        for var in variables:
            for year in years:
                if model == 'era5':
                    path = Path(f'data/archive/era5/{var}/{year}.nc')
                else:
                    path = Path(f'data/archive/cmip6/{model}/{var}/{year}.nc')
                path.parent.mkdir(parents=True, exist_ok=True)
                time = pd.date_range(f'{year}-01-01', periods=12, freq='MS')
                ds = xr.Dataset(
                    {var: (('time', 'lat', 'lon'),
                           rng.normal(0, 1, (len(time), len(lat), len(lon))))},
                    coords={'time': time, 'lat': lat, 'lon': lon},
                )
                ds.to_netcdf(path, engine='h5netcdf')


def main(argv):
    which = argv[1:] or ['ex1', 'ex2', 'ex3', 'ex4', 'ex8']
    for name in which:
        print(f'generating data for {name}')
        globals()[name]()


if __name__ == '__main__':
    main(sys.argv)
