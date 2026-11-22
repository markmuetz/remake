"""The tiny pipeline behind the old-DB fixtures (tests/fixtures/old_dbs/).

Plain remake 0.8 API only, so every release from 0.8.5 on can run it.
Two rules, four tasks. Rebuild the fixtures with make_fixtures.py.
"""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()


@rule(outputs={'o': 'data/part_{i}.txt'}, matrix={'i': [1, 2, 3]})
def fx_make(outputs, i):
    """Write one small file per i."""
    Path(outputs['o']).write_text(f'part {i}\n')


@rule(inputs={f'p{i}': f'data/part_{i}.txt' for i in [1, 2, 3]},
      outputs={'o': 'data/total.txt'}, depends_on=[fx_make])
def fx_total(inputs, outputs):
    """Concatenate the parts."""
    Path(outputs['o']).write_text(''.join(Path(p).read_text() for _, p in sorted(inputs.items())))


rmk.rules_from_current_module()
