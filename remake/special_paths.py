from pathlib import Path


def is_relative_to(p1, p2):
    # Python <= 3.8 doesn't have this as a method on Path.
    try:
        p1.relative_to(p2)
        return True
    except ValueError:
        return False


class SpecialPaths:
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