"""remake tutorial, lesson 2: a matrix — one task per site and year."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()

SITES = ['aberdeen', 'cambridge', 'durham', 'exeter']
YEARS = [2020, 2021, 2022, 2023]


@rule(
    inputs={'raw': 'data/raw/{site}/{year}.csv'},
    outputs={'clean': 'data/clean/{site}/{year}.csv'},
    matrix={'site': SITES, 'year': YEARS},
)
def clean(inputs, outputs, site, year):
    """Drop the days with a missing reading."""
    rows = Path(inputs['raw']).read_text().splitlines()
    kept = [row for row in rows if not row.endswith(',NA')]
    Path(outputs['clean']).write_text('\n'.join(kept) + '\n')


rmk.rules_from_current_module()
