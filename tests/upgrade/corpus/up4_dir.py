"""Upgrade corpus 4: a Dir output, and a Dir that is a symlink to another
directory (check_upgrade.py creates the link data/up4/linked -> data/up4/target
before any run)."""
from pathlib import Path

from remake import Dir, Remake, rule

rmk = Remake()


@rule(outputs={'d': Dir('data/up4/plain')})
def up4_plain(outputs):
    """Write two files into a Dir."""
    for i in range(2):
        (Path(outputs['d']) / f'f{i}.txt').write_text(f'{i}\n')


@rule(outputs={'d': Dir('data/up4/linked')})
def up4_linked(outputs):
    """Write into a Dir that is a symlink to another directory."""
    (Path(outputs['d']) / 'g.txt').write_text('linked\n')


rmk.rules_from_current_module()
