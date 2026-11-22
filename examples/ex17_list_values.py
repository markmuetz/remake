"""ex17: list-valued inputs and outputs (remake 0.10).

One key can name several paths: a list of templates, each formatted with the
task's matrix values. The rule body gets a list (of paths for inputs, of
path-like tokens for outputs); lint, `task-info`, `ls-tasks`, the
missing-inputs check and dependency tracking see every element, shown as
`key[0]`, `key[1]`, ...

Try:

    remake run ex17_list_values.py
    remake task-info ex17_list_values.py -Q "rule == 'pair_up17' and site == 'a'"
    remake lint ex17_list_values.py

Queries (`-Q`) are a small restricted language: comparisons of kwarg names and
literals, joined with and / or / not. No function calls or arithmetic:

    remake info ex17_list_values.py -Q "site in ['a', 'b'] and rule != 'summary17'"
    remake info ex17_list_values.py -Q "not site == 'c'"
    remake info ex17_list_values.py -Q True            # every task
    remake info ex17_list_values.py -Q "len(site) == 1"   # error (exit 2)

Lists must be flat (a nested list is an error at definition); use separate
keys for separate groups. Stdlib only.
"""
from pathlib import Path

from remake import Remake, rule

SITES = ['a', 'b', 'c']


@rule(outputs={'halves': ['data/ex17/{site}_first.txt', 'data/ex17/{site}_second.txt']},
      matrix={'site': SITES})
def split17(outputs, site):
    """Write two files per site, declared as one list-valued output."""
    first, second = outputs['halves']
    Path(first).write_text(f'{site}1\n')
    Path(second).write_text(f'{site}2\n')


@rule(inputs={'halves': ['data/ex17/{site}_first.txt', 'data/ex17/{site}_second.txt']},
      outputs={'joined': 'data/ex17/{site}_joined.txt'},
      matrix={'site': SITES}, depends_on=[split17])
def pair_up17(inputs, outputs, site):
    """Read both halves (a list of input paths) and join them."""
    Path(outputs['joined']).write_text(''.join(Path(p).read_text() for p in inputs['halves']))


@rule(inputs=lambda: {'joined': [f'data/ex17/{s}_joined.txt' for s in SITES]},
      outputs={'summary': 'data/ex17/summary17.txt'}, depends_on=[pair_up17])
def summarise17(inputs, outputs):
    """Fan in: a callable input spec can also return a list."""
    lines = [Path(p).read_text().replace('\n', ' ').strip() for p in inputs['joined']]
    Path(outputs['summary']).write_text('\n'.join(lines) + '\n')


rmk = Remake()
rmk.rules_from_current_module()
