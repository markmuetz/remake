from pathlib import Path
from typing import Union

from remake.util import load_module
from remake.remake_exceptions import RemakeLoadError


# Remake cannot be loaded until body of function - ignore flake8 error.
def load_remake(filename: Union[str, Path], finalize: bool = False) -> 'Remake':  # noqa: F821
    """Load a remake instance from a file.

    >>> ex1 = load_remake('examples/ex1.py')
    >>> ex1.finalized
    False

    :param filename: file that contains exactly one `remake = Remake()`
    :param finalize: finalize the remake instance
    :return: instance of `Remake`
    """
    # Avoids circular import.
    from remake import Remake
    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')

    remake_module = load_module(filename, {'__remake__': '__old__'})
    # remakes = [o for o in [getattr(remake_module, m) for m in dir(remake_module)]
    #            if o.__class__.__name__ == 'Remake']
    remakes = [o for o in [getattr(remake_module, m) for m in dir(remake_module)]
               if isinstance(o, Remake)]
    if len(remakes) > 1:
        raise RemakeLoadError(f'More than one remake defined in {filename}')
    elif not remakes:
        raise RemakeLoadError(f'No remake defined in {filename}')
    if finalize:
        return remakes[0].finalize()
    else:
        return remakes[0]
