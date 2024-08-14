import inspect
from pathlib import Path
from typing import Union

from remake.util import load_module
from remake.remake_exceptions import RemakeLoadError


def load_remake(filename, finalize=False):
    # Avoids circular import.
    from .remake import Remake
    from .rule import Rule
    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    remake_module = load_module(filename)
    module_vars = [getattr(remake_module, m) for m in dir(remake_module)]
    remakes = [o for o in module_vars
               if isinstance(o, Remake)]
    rules = [o for o in module_vars
             if inspect.isclass(o) and issubclass(o, Rule) and not o is Rule]
    if len(remakes) > 1:
        raise RemakeLoadError(f'More than one remake defined in {filename}')
    elif not remakes:
        raise RemakeLoadError(f'No remake defined in {filename}')
    rmk = remakes[0]
    print(rules)
    rmk.load_rules(rules)
    if finalize:
        rmk.finalize()
    return rmk
