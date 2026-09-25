"""remake tutorial, lesson 1: one rule, one task."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()


@rule(
    inputs={'raw': 'data/raw/aberdeen/2020.csv'},
    outputs={'clean': 'data/clean/aberdeen/2020.csv'},
)
def clean(inputs, outputs):
    """Drop the days with a missing reading."""
    rows = Path(inputs['raw']).read_text().splitlines()
    kept = [row for row in rows if not row.endswith(',NA')]
    Path(outputs['clean']).write_text('\n'.join(kept) + '\n')


rmk.rules_from_current_module()
