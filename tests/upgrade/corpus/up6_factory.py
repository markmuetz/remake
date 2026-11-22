"""Upgrade corpus 6: a rule factory with name= and uses."""
from pathlib import Path

from remake import Remake, rule

rmk = Remake()


def up6_double(x):
    """Double."""
    return 2 * x


def up6_negate(x):
    """Negate."""
    return -x


def make_up6_rule(op_name, op):
    """One rule per operation, distinguished by name=."""
    @rule(name=f'up6_{op_name}', outputs={'o': f'data/up6/{op_name}_{{i}}.txt'},
          matrix={'i': [1, 2]}, uses={'op': op})
    def op_rule(outputs, i):
        """Write op(i)."""
        Path(outputs['o']).write_text(f'{op(i)}\n')
    return op_rule


rmk.add_rules([make_up6_rule('double', up6_double), make_up6_rule('negate', up6_negate)])
