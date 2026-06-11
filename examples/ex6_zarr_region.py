# remake3_examples/ex6_zarr_region.py
#
# Demonstrates optional inputs and outputs:
#   - create_store: a source rule (no inputs) that initialises an empty
#     zarr store spanning the full time axis.
#   - write_year: a side-effect rule (no outputs) — each task writes one
#     year's slab into a region of the shared store. There is no per-task
#     artefact to declare, and no dummy sentinel files. Completion is
#     tracked in the metadata DB.
#   - log_summary: a rule with neither inputs nor outputs — a pure side
#     effect (here, an upsert into an external database).
#
# Note rmk = Remake() at the top, rmk.rules_from_current_module() at the
# bottom, after all rules are defined.
#
# DAG: create_store --> write_year[year] --> log_summary

import datetime as dt

import numpy as np
import pandas as pd
import xarray as xr

from remake3 import Remake, ZarrStore, rule

rmk = Remake()

YEARS = list(range(1980, 2024))
STORE = ZarrStore('data/temperature.zarr')


@rule(outputs={'store': STORE})
def create_store(outputs):
    """Source rule: no inputs. Lays down the full coordinate grid with
    empty (lazily-allocated) data, then consolidates metadata."""
    import zarr
    time = pd.date_range(f'{YEARS[0]}-01-01', f'{YEARS[-1]}-12-31', freq='D')
    ds = xr.Dataset(
        {'temp': (('time', 'lat', 'lon'),
                  np.full((len(time), 180, 360), np.nan, dtype='f4'))},
        coords={'time': time,
                'lat': np.arange(-89.5, 90),
                'lon': np.arange(-179.5, 180)},
    )
    ds.chunk({'time': 365}).to_zarr(outputs['store'], mode='w', compute=False)
    zarr.consolidate_metadata(outputs['store'])


@rule(
    inputs     = create_store.outputs,
    matrix     = {'year': YEARS},
    depends_on = [create_store],
)
def write_year(inputs, year):
    """Side-effect rule: no outputs. Writes one year's data into its
    region of the shared store. The store itself belongs to create_store;
    a region is not independently addressable, so nothing is declared."""
    ds = xr.open_zarr(inputs['store'])
    time_index = ds.get_index('time')
    start = time_index.get_loc(dt.datetime(year, 1, 1))
    stop = time_index.get_loc(dt.datetime(year, 12, 31)) + 1

    year_ds = compute_year(year, ds.lat, ds.lon, time_index[start:stop])
    year_ds.to_zarr(inputs['store'], region={'time': slice(start, stop)})


@rule(depends_on=[write_year])
def log_summary():
    """Neither inputs nor outputs: pure side effect. Records run metadata
    in an external database — nothing on disk to declare or check."""
    import sqlite3
    con = sqlite3.connect('data/pipeline_log.db')
    con.execute('CREATE TABLE IF NOT EXISTS runs (completed_at TEXT, n_years INT)')
    con.execute('INSERT INTO runs VALUES (?, ?)',
                (dt.datetime.now().isoformat(), len(YEARS)))
    con.commit()


def compute_year(year, lat, lon, time):
    rng = np.random.default_rng(year)
    return xr.Dataset(
        {'temp': (('time', 'lat', 'lon'),
                  rng.normal(15, 10, (len(time), len(lat), len(lon))).astype('f4'))},
        coords={'time': time, 'lat': lat, 'lon': lon},
    )


rmk.rules_from_current_module()
