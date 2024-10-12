import sys
import importlib
import inspect
from pathlib import Path
from typing import Union

def load_module(local_filename: Union[str, Path], module_attrs=None):
    """Use Python internals to load a Python module from a filename.

    >>> load_module('examples/ex1.py').__name__
    'ex1'

    :param local_filename: name of module to load
    :return: module
    """
    module_path = Path.cwd() / local_filename
    if not module_path.exists():
        raise Exception(f'Module file {module_path} does not exist')

    # No longer needed due to sys.modules line below.
    # Make sure any local imports in the module script work.
    sys.path.append(str(module_path.parent))
    module_name = Path(local_filename).stem

    try:
        # See: https://stackoverflow.com/a/50395128/54557
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        if module_attrs:
            for k, v in module_attrs.items():
                setattr(module, k, v)
        spec.loader.exec_module(module)
    except SyntaxError:
        print(f'Bad syntax in module file {module_path}')
        raise

    return module


def load_remake(filename, finalize=True, run=False):
    # Avoids circular import.
    from ..core import Remake, Rule
    from ..core.remake_exceptions import RemakeLoadError



    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    remake_module = load_module(filename, {'__remake__': '__new__', '__remake_run__': run})
    module_vars = [getattr(remake_module, m) for m in dir(remake_module)]
    remakes = [o for o in module_vars if isinstance(o, Remake)]
    rules = [o for o in module_vars if inspect.isclass(o) and issubclass(o, Rule) and not o is Rule]
    if len(remakes) > 1:
        raise RemakeLoadError(f'More than one remake defined in {filename}')
    elif not remakes:
        raise RemakeLoadError(f'No remake defined in {filename}')
    rmk = remakes[0]
    rmk.load_rules(rules, finalize)
    return rmk


def load_archive(filename):
    # Avoids circular import.
    from ..core import Remake, Rule
    from ..core.remake_exceptions import RemakeLoadError

    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    archive_module = load_module(filename, {})
    module_vars = [getattr(archive_module, m) for m in dir(archive_module)]
    archives = [o for o in module_vars if isinstance(o, BaseArchive)]
    if len(archives) > 1:
        raise RemakeLoadError(f'More than one archive defined in {filename}')
    elif not archives:
        raise RemakeLoadError(f'No archive defined in {filename}')
    archive = archives[0]
    return archive
