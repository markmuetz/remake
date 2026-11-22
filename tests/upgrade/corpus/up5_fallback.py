"""Upgrade corpus 5: check_outputs='fallback'."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake(check_outputs='fallback')


@rule(outputs={'o': 'data/up5/{i}.txt'}, matrix={'i': [1, 2]})
def up5_a(outputs, i):
    """Write a file."""
    Path(outputs['o']).write_text(f'{i}\n')


@rule(inputs={'x': 'data/up5/{i}.txt'}, outputs={'o': 'data/up5/b_{i}.txt'},
      matrix={'i': [1, 2]}, depends_on=[up5_a])
def up5_b(inputs, outputs, i):
    """Copy the file."""
    Path(outputs['o']).write_text(Path(inputs['x']).read_text())


rmk.rules_from_current_module()
