from pathlib import Path


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

