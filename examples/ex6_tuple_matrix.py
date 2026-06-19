# examples/ex6_tuple_matrix.py
#
# Tuple-key matrices: an explicit, pre-filtered sequence of parameter combos
# instead of the full cartesian product.
#
# A full {model} x {year} grid would be 4 x 4 = 16 combos, but not every model
# has data for every year. A tuple key binds several kwargs together, so you
# build the exact (model, year) pairs you want -- typically by filtering a long
# candidate sequence against what data actually exists -- and remake expands
# one task per pair, no missing-data combos.
#
#   {('model', 'year'): [(m, y), ...]}   one task per listed pair
#
# Tuple and scalar keys mix freely: each dict entry is one product axis, so
# `combine` below takes the cartesian product of the `region` scalar axis with
# the grouped (model, year) axis.
#
# DAG: process[model, year] --> combine[region, model, year]

import json
from pathlib import Path

from remake import Remake, rule

rmk = Remake()

MODELS = ['cesm', 'gfdl', 'ipsl', 'miroc']
YEARS = [2000, 2005, 2010, 2015]
REGIONS = ['arctic', 'tropics']

# Which (model, year) combinations actually have input data. In a real pipeline
# you would discover this at module load (e.g. globbing a directory) rather than
# hard-coding it -- the point is that it is a sparse subset of the full grid.
AVAILABLE = {
    ('cesm', 2000), ('cesm', 2005), ('cesm', 2010), ('cesm', 2015),
    ('gfdl', 2005), ('gfdl', 2010),
    ('ipsl', 2010), ('ipsl', 2015),
    ('miroc', 2015),
}

# Filter the full candidate grid down to available pairs -> the tuple-key axis.
MODEL_YEARS = [(m, y) for m in MODELS for y in YEARS if (m, y) in AVAILABLE]


@rule(
    outputs = {'series': 'data/series/{model}_{year}.json'},
    matrix  = {('model', 'year'): MODEL_YEARS},   # 9 tasks, not 16
)
def process(outputs, model, year):
    # Synthetic, deterministic "data" so the example is self-contained.
    value = (hash((model, year)) % 1000) / 10.0
    Path(outputs['series']).write_text(json.dumps({'model': model, 'year': year,
                                                    'value': value}))


@rule(
    inputs     = process.outputs,
    outputs    = {'combined': 'data/combined/{region}_{model}_{year}.json'},
    # Mixed axes: cartesian product of the region scalar axis with the grouped
    # (model, year) axis -> 2 x 9 = 18 tasks.
    matrix     = {'region': REGIONS, ('model', 'year'): MODEL_YEARS},
    depends_on = [process],
)
def combine(inputs, outputs, region, model, year):
    series = json.loads(Path(inputs['series']).read_text())
    series['region'] = region
    Path(outputs['combined']).write_text(json.dumps(series))


rmk.rules_from_current_module()
