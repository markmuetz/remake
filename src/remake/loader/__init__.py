import importlib.util
import sys
from pathlib import Path
from typing import Union


def load_module(local_filename: Union[str, Path], module_attrs=None):
    """Use Python internals to load a Python module from a filename.

    :param local_filename: name of module to load
    :return: module
    """
    module_path = Path.cwd() / local_filename
    if not module_path.exists():
        raise FileNotFoundError(f'Module file {module_path} does not exist')

    # Make sure any local imports in the module script work.
    sys.path.append(str(module_path.parent))
    module_name = Path(local_filename).stem

    # See: https://stackoverflow.com/a/50395128/54557
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    if module_attrs:
        for k, v in module_attrs.items():
            setattr(module, k, v)
    spec.loader.exec_module(module)

    return module


def load_remake(filename, finalize=True):
    """Load a pipeline file and return its Remake instance."""
    # Avoids circular import.
    from ..core.exceptions import RemakeLoadError
    from ..core.remake import Remake

    filename = Path(filename)
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    remake_module = load_module(filename)
    remakes = [o for o in vars(remake_module).values() if isinstance(o, Remake)]
    if len(remakes) > 1:
        raise RemakeLoadError(f'More than one Remake defined in {filename}')
    if not remakes:
        raise RemakeLoadError(f'No Remake defined in {filename}')
    rmk = remakes[0]
    # The path the user refers to this pipeline by — executors that generate
    # scripts re-invoking remake (SLURM) need it.
    rmk.remakefile = str(filename)
    if finalize:
        rmk.finalize()
    return rmk
