from pathlib import Path, PosixPath  # noqa: F401


def is_relative_to(p1, p2):
    # Python <= 3.8 doesn't have this as a method on Path.
    try:
        p1.relative_to(p2)
        return True
    except ValueError:
        return False


class SpecialPaths:
    """Special paths to use for all input/output filenames.

    When tasks use inputs or create outputs, they are referenced by their filesystem path.
    This class makes it easy to define special paths that are used internally to locate the actual file.
    For example, `CWD` is a special path for the current working directory.
    This can be used to make paths consistent across different machine. If machine A has a file at path /A/data/path,
    and machine B has a file at path /B/data/path, a special path called `DATA` could be set up, pointing to the right
    path on each machine. E.g. on machine A:

    >>> special_paths = SpecialPaths(DATA='/A/data')
    >>> special_paths.DATA
    PosixPath('/A/data')

    This must be passed into `Remake` to take effect:

    >>> from remake import Remake
    >>> demo = Remake(special_paths=special_paths)
    """
    def __init__(self, **paths):
        if 'CWD' not in paths:
            paths['CWD'] = Path.cwd()
        for k, v in paths.items():
            assert isinstance(k, str), f'{k} not a string'
            assert isinstance(v, Path) or isinstance(v, str), f'{v} not a Path or string'
            setattr(self, k, Path(v))
            paths[k] = Path(v).absolute()
        # Make sure longer paths come higher up the list.
        self.paths = dict(sorted(paths.items(), key=lambda x: len(x[1].parts))[::-1])

    def __repr__(self):
        arg = ', '.join([f'{k}={repr(v)}' for k, v in self.paths.items()])
        return f'Paths({arg})'


def map_special_paths(special_paths, paths):
    """Utility function to map all paths using special paths.

    i.e. if a path is /A/data/path, it would be mapped to DATA/path:
    >>> special_paths = SpecialPaths(DATA='/A/data')
    >>> map_special_paths(special_paths, {'path1': Path('/A/data/path')})
    {'path1': PosixPath('DATA/path')}
    """
    mapped_paths = {}
    for path_name, path in paths.items():
        mapped_path = None
        for special_path_name, special_path in special_paths.paths.items():
            if is_relative_to(path, special_path.absolute()):
                mapped_path = Path(special_path_name) / path.relative_to(special_path)
                break
        if mapped_path:
            mapped_paths[path_name] = mapped_path
        else:
            mapped_paths[path_name] = path

    return mapped_paths
