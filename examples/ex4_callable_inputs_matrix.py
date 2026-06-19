# examples/ex4_callable_inputs_matrix.py
#
# Shows the full range of input/output specification styles:
#   - plain dict (static)
#   - format-string dict (interpolated per task)
#   - callable returning a dict (full Python logic)
#   - `uses` to track a lookup table defined outside the rule
#
# Also demonstrates matrix inheritance and subsetting.

from pathlib import Path
from remake import Remake, rule

rmk = Remake()

# A lookup table defined at module level — tracked via `uses`
SENSOR_CALIBRATION = {
    'site_a': 1.02,
    'site_b': 0.98,
    'site_c': 1.00,
}

SITES   = list(SENSOR_CALIBRATION.keys())
YEARS   = [2020, 2021, 2022]
SEASONS = ['DJF', 'MAM', 'JJA', 'SON']


# --- style 1: plain dict, no matrix (omitted → single task) ---

@rule(
    inputs  = {'config': 'config/pipeline.yaml'},
    outputs = {'validated': 'config/validated.yaml'},
)
def validate_config(inputs, outputs):
    import yaml
    cfg = yaml.safe_load(Path(inputs['config']).read_text())
    assert 'version' in cfg
    Path(outputs['validated']).write_text(yaml.dump(cfg))


# --- style 2: format-string dict, simple matrix ---

@rule(
    inputs  = {'raw': 'data/raw/{site}/{year}.csv'},
    outputs = {'cal': 'data/calibrated/{site}/{year}.csv'},
    matrix  = {'site': SITES, 'year': YEARS},
    uses    = {'calibration': SENSOR_CALIBRATION},
)
def calibrate(inputs, outputs, site, year):
    import csv
    factor = calibration[site]
    rows = list(csv.DictReader(Path(inputs['raw']).open()))
    with Path(outputs['cal']).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['site', 'year', 'value'])
        w.writeheader()
        for r in rows:
            w.writerow({'site': site, 'year': year,
                        'value': float(r['value']) * factor})


# --- style 3: callable inputs, more complex matrix ---
# Each seasonal slice requires all months in that season.

SEASON_MONTHS = {
    'DJF': [12, 1, 2],
    'MAM': [3, 4, 5],
    'JJA': [6, 7, 8],
    'SON': [9, 10, 11],
}

def seasonal_inputs(site, year, season):
    """DJF wraps across year boundary: Dec is from prior year.

    Years outside the calibrated range are omitted — seasonal_means
    skips missing months (DJF of the first year has no December).
    """
    months = SEASON_MONTHS[season]
    result = {}
    for m in months:
        y = year - 1 if (season == 'DJF' and m == 12) else year
        if y in YEARS:
            result[f'm{m:02d}'] = f'data/calibrated/{site}/{y}.csv'
    return result


@rule(
    inputs     = seasonal_inputs,
    outputs    = {'seasonal': 'data/seasonal/{site}/{year}/{season}.csv'},
    matrix     = {'site': SITES, 'year': YEARS, 'season': SEASONS},
    depends_on = [calibrate],
    uses       = {'season_months': SEASON_MONTHS},
)
def seasonal_means(inputs, outputs, site, year, season):
    import csv, statistics
    all_values = []
    months = season_months[season]
    for m in months:
        key = f'm{m:02d}'
        if key not in inputs:
            continue
        rows = list(csv.DictReader(Path(inputs[key]).open()))
        all_values.extend(float(r['value']) for r in rows)
    with Path(outputs['seasonal']).open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['site', 'year', 'season', 'mean', 'n'])
        w.writerow([site, year, season,
                    f'{statistics.mean(all_values):.4f}', len(all_values)])


# --- style 4: inheriting and subsetting a matrix ---
# Annual summary only needs site × year, not season.

def annual_inputs(site, year):
    return {s: f'data/seasonal/{site}/{year}/{s}.csv' for s in SEASONS}


@rule(
    inputs     = annual_inputs,
    outputs    = {'annual': 'data/annual/{site}/{year}.csv'},
    matrix     = {'site': SITES, 'year': YEARS},   # subset of seasonal matrix
    depends_on = [seasonal_means],
)
def annual_summary(inputs, outputs, site, year):
    import csv, statistics
    all_means = []
    for path in inputs.values():
        rows = list(csv.DictReader(Path(path).open()))
        all_means.extend(float(r['mean']) for r in rows)
    with Path(outputs['annual']).open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['site', 'year', 'annual_mean'])
        w.writerow([site, year, f'{statistics.mean(all_means):.4f}'])


rmk.rules_from_current_module()
