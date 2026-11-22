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
    # Register so inspect.getsource works on classes defined in the module
    # (it resolves a class's file via sys.modules[cls.__module__]; functions
    # don't need this). Never shadow an already-imported module of the same
    # name — a remakefile called json.py must not break stdlib imports.
    if module_name not in sys.modules:
        sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as e:
        # A missing package is a setup problem, not a remake bug: report it
        # as a clean error (exit 2), with the traceback at DEBUG. Other
        # ImportErrors (circular imports, `cannot import name` deep in user
        # code) keep their traceback.
        from loguru import logger

        from ..core.exceptions import RemakeLoadError

        logger.opt(exception=e).debug('importing {} failed', local_filename)
        raise RemakeLoadError(
            f'could not import remakefile {local_filename}: {type(e).__name__}: {e}') from e

    return module


def load_remake(filename, finalize=True, profile=None):
    """Load a pipeline file and return its Remake instance."""
    # Avoids circular import.
    from ..core.exceptions import RemakeLoadError
    from ..core.remake import Remake

    filename = Path(filename)
    if not filename.name or filename.is_dir():
        raise RemakeLoadError(f'Expected a remakefile (.py), got a directory: {filename}')
    if not filename.suffix:
        filename = filename.with_suffix('.py')
    if filename.suffix != '.py':
        raise RemakeLoadError(f'Not a Python remakefile (expected a .py file): {filename}')
    if not (Path.cwd() / filename).is_file():
        raise RemakeLoadError(f'Remakefile not found: {Path.cwd() / filename}')
    remake_module = load_module(filename)
    remakes = [o for o in vars(remake_module).values() if isinstance(o, Remake)]
    if len(remakes) > 1:
        raise RemakeLoadError(f'More than one Remake defined in {filename}')
    if not remakes:
        raise RemakeLoadError(f'No Remake defined in {filename}')
    rmk = remakes[0]
    # The path the user refers to this pipeline by — executors that generate
    # scripts re-invoking remake (SLURM) need it.
    # A bare name (the CLI, which has changed into its directory) stays as
    # is; a path with a directory part is made absolute, since methods run
    # in the remakefile's directory and workers re-load it from there
    # (review 2026-11-22, finding 5).
    rmk.remakefile = (str(filename) if Path(filename).parent == Path('.')
                      else str((Path.cwd() / filename).resolve()))
    # Anchored at the remakefile's directory (D6), however it was found.
    rmk.project_dir = str((Path.cwd() / filename).parent.resolve())
    # Merge the config cascade once, with remake.toml from the remakefile's
    # directory (before finalize: nothing may read a stale config).
    rmk.resolve_config((Path.cwd() / filename).parent, profile=profile)
    if finalize:
        rmk.finalize()
    return rmk
