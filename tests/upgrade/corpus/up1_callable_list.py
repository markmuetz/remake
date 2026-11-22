"""Upgrade corpus 1: a callable inputs spec returning a list of Paths."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()


@rule(outputs={'f': 'data/up1/{i}.txt'}, matrix={'i': [0, 1, 2]})
def up1_make(outputs, i):
    """Write one small file per i."""
    Path(outputs['f']).write_text(f'{i}\n')


def up1_inputs():
    """A callable spec whose value is a list of Path objects."""
    return {'files': [Path(f'data/up1/{i}.txt') for i in range(3)]}


@rule(inputs=up1_inputs, outputs={'total': 'data/up1/total.txt'},
      depends_on=[up1_make])
def up1_collect(inputs, outputs):
    """Concatenate every listed file."""
    Path(outputs['total']).write_text(''.join(Path(p).read_text() for p in inputs['files']))


rmk.rules_from_current_module()
