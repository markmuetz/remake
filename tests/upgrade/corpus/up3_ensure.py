"""Upgrade corpus 3: Ensure(..., nonempty=True) on a file output."""
from pathlib import Path

from remake import Ensure, Remake, rule

rmk = Remake()


@rule(outputs={'o': Ensure('data/up3/{i}.txt', nonempty=True)}, matrix={'i': [1, 2]})
def up3_ensure(outputs, i):
    """Write a non-empty file."""
    Path(outputs['o']).write_text(f'content {i}\n')


rmk.rules_from_current_module()
