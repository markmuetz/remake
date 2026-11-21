"""Configuration cascade: built-in defaults < profile < user file < project
file < `Remake(config=)` (< per-rule `@rule(config=)`, merged by the
executors). See docs/guide/configuration.md.

Dicts merge recursively; lists and scalars replace. Unknown keys are errors
in files (warnings for `Remake(config=)`). Every leaf key records which layer
set it (`EffectiveConfig.provenance`).
"""
import copy
import os
import sys
from pathlib import Path

from loguru import logger

from ..core.exceptions import RemakeError

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

# The built-in defaults (layer a). Same values the SLURM executor has always
# used; to be removed at 1.0 in favour of an explicit profile.
BUILTIN_DEFAULTS = {
    'slurm': {
        'partition': 'standard',
        'qos': 'standard',
        'time': '4:00:00',
        'mem': '4G',
    },
}

_SLURM_VALUE_TYPES = (str, int, float, bool)

# key -> 'slurm' (a table of any sbatch options), a dict (a table with fixed
# keys -> allowed types), or a tuple of types (a leaf).
_SCHEMA = {
    'slurm': 'slurm',
    'multiproc': {'nproc': (int,)},
    'dask': {'scheduler': (str,), 'nproc': (int,)},
    'resources': {'capture': (bool,), 'rss_interval': (int, float)},
    'external_inputs_hint': (str,),
    'checksum_outputs': (bool,),
}
# Keys allowed in the user file (per-user machine settings, not pipeline
# settings), and in a [profiles.X] table there.
USER_KEYS = frozenset({'slurm', 'multiproc', 'dask', 'resources'})
_PROJECT_KEYS = frozenset(_SCHEMA)
_FILE_ONLY = ('profile', 'profiles')


def user_config_path():
    base = os.environ.get('XDG_CONFIG_HOME')
    if not base:
        base = os.path.join(os.path.expanduser('~'), '.config')
    return Path(base) / 'remake' / 'config.toml'


def shipped_profile_dir():
    return Path(__file__).resolve().parent.parent / 'profiles'


def shipped_profile_names():
    d = shipped_profile_dir()
    return sorted(p.stem for p in d.glob('*.toml')) if d.is_dir() else []


def _isinstance(v, types):
    if isinstance(v, bool) and bool not in types:
        return False
    return isinstance(v, types)


def _tname(types):
    names = {str: 'a string', int: 'an integer', float: 'a number', bool: 'a boolean'}
    return ' or '.join(names[t] for t in types)


def _problems(cfg, allowed):
    """Messages for unknown keys / wrong types in `cfg`, with dotted paths."""
    out = []
    for key, val in cfg.items():
        if key not in allowed:
            out.append(f'unknown key {key!r}')
            continue
        schema = _SCHEMA[key]
        if schema == 'slurm':
            if not isinstance(val, dict):
                out.append(f'{key!r} must be a table')
                continue
            for k, v in val.items():
                if not _isinstance(v, _SLURM_VALUE_TYPES):
                    out.append(f'{key}.{k} must be a string, number or boolean '
                               f'(an sbatch option), not {type(v).__name__}')
        elif isinstance(schema, dict):
            if not isinstance(val, dict):
                out.append(f'{key!r} must be a table')
                continue
            for k, v in val.items():
                if k not in schema:
                    out.append(f"unknown key '{key}.{k}'")
                elif not _isinstance(v, schema[k]):
                    out.append(f'{key}.{k} must be {_tname(schema[k])}, '
                               f'not {type(v).__name__}')
        elif not _isinstance(val, schema):
            out.append(f'{key!r} must be {_tname(schema)}, not {type(val).__name__}')
    return out


def validate_programmatic(cfg):
    """Validate a `Remake(config=)` dict: unknown keys and bad types warn
    (programmatic callers may stash their own keys); never raises."""
    for msg in _problems(cfg, _PROJECT_KEYS):
        logger.warning(f'Remake(config=): {msg}')


def _validate_file(cfg, source, allowed):
    probs = _problems(cfg, allowed)
    if probs:
        raise RemakeError(f'{source}: ' + '; '.join(probs))


def _load_toml(path):
    try:
        with open(path, 'rb') as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise RemakeError(f'{path}: invalid TOML: {e}') from None
    except OSError as e:
        raise RemakeError(f'{path}: cannot read: {e}') from None


def _read_layer(path, allowed):
    """Parse and validate one config file ({} when absent): the settings
    keys in `allowed`, plus `profile` and `[profiles.*]` tables (whose
    contents may use the same keys)."""
    if not path.is_file():
        return {}
    cfg = _load_toml(path)
    _validate_file({k: v for k, v in cfg.items() if k not in _FILE_ONLY}, path, allowed)
    if not isinstance(cfg.get('profile', ''), str):
        raise RemakeError(f"{path}: 'profile' must be a string")
    profiles = cfg.get('profiles', {})
    if not isinstance(profiles, dict) or not all(isinstance(v, dict) for v in profiles.values()):
        raise RemakeError(f"{path}: 'profiles' must be a table of tables")
    for name, prof in profiles.items():
        _validate_file(prof, f'{path} [profiles.{name}]', allowed)
    return cfg


def merge(base, over, label, provenance, path=''):
    """Merge `over` into `base` in place: dicts recursively, everything else
    replaces. Records `label` against each leaf key set from `over`."""
    for k, v in over.items():
        p = f'{path}{k}'
        if isinstance(v, dict):
            if not isinstance(base.get(k), dict):
                base[k] = {}
            merge(base[k], v, label, provenance, p + '.')
        else:
            base[k] = copy.deepcopy(v)
            provenance[p] = label


class EffectiveConfig:
    """The merged config plus where each key came from."""

    def __init__(self, values, provenance, profile, profile_source, files):
        self.values = values
        self.provenance = provenance  # 'slurm.mem' -> 'user file' etc.
        self.profile = profile  # selected profile name, or None
        self.profile_source = profile_source  # how it was selected, or None
        self.files = files  # [(layer, Path, exists)]

    def to_json(self):
        return {'profile': self.profile, 'profile_source': self.profile_source,
                'files': [{'layer': layer, 'path': str(p), 'exists': e}
                          for layer, p, e in self.files],
                'config': self.values, 'provenance': self.provenance}


def resolve(project_dir=None, *, programmatic=None, profile=None):
    """Build the effective config. `project_dir` is where `remake.toml`
    lives (the remakefile's directory; default cwd); `profile` is the CLI
    selection, which beats REMAKE_PROFILE and the files."""
    project_dir = Path(project_dir) if project_dir is not None else Path.cwd()
    user_path = user_config_path()
    project_path = project_dir / 'remake.toml'
    user = _read_layer(user_path, USER_KEYS)
    project = _read_layer(project_path, _PROJECT_KEYS)
    files = [('user file', user_path, user_path.is_file()),
             ('project file', project_path, project_path.is_file())]

    name, source = None, None
    for cand, src in ((profile, 'cli --profile'),
                      (os.environ.get('REMAKE_PROFILE'), 'env REMAKE_PROFILE'),
                      (project.get('profile'), 'project file'),
                      (user.get('profile'), 'user file')):
        if cand:
            name, source = cand, src
            break

    values, prov = {}, {}
    merge(values, BUILTIN_DEFAULTS, 'builtin default', prov)

    if name is not None:
        parts = []  # (label, dict): shipped, then user's, then project's
        shipped = shipped_profile_dir() / f'{name}.toml'
        if shipped.is_file():
            cfg = _load_toml(shipped)
            _validate_file(cfg, shipped, _PROJECT_KEYS)
            parts.append((f'profile {name}', cfg))
        for lbl, layer in (('user file', user), ('project file', project)):
            if name in layer.get('profiles', {}):
                parts.append((f'profile {name} ({lbl})', layer['profiles'][name]))
        if not parts:
            avail = sorted(set(shipped_profile_names()) | set(user.get('profiles', {}))
                           | set(project.get('profiles', {})))
            raise RemakeError(
                f'Unknown profile {name!r} (selected by {source}); available: '
                f'{", ".join(avail) or "none"}')
        for lbl, cfg in parts:
            merge(values, cfg, lbl, prov)

    for lbl, layer in (('user file', user), ('project file', project)):
        merge(values, {k: v for k, v in layer.items() if k not in _FILE_ONLY}, lbl, prov)
    if programmatic:
        merge(values, programmatic, 'Remake(config=)', prov)
    return EffectiveConfig(values, prov, name, source, files)
