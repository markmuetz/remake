"""Upgrade corpus 2: uses= with a constant and a helper function."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()

SCALE = 3


def up2_helper(x):
    """Scale a number."""
    return x * SCALE


@rule(outputs={'o': 'data/up2/{i}.txt'}, matrix={'i': [1, 2, 3]},
      uses={'SCALE': SCALE, 'helper': up2_helper})
def up2_scale(outputs, i):
    """Write the scaled value."""
    Path(outputs['o']).write_text(f'{up2_helper(i)}\n')


rmk.rules_from_current_module()
