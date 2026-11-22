"""Where remake keeps its metadata: the one source of truth for `.remake/`.

By default the metadata directory is `.remake` relative to the cwd, which is
the project directory (the remakefile's) under the CLI and inside any
`Remake` method (0.10 D6). The `metadata_dir` key in `remake.toml` (or
`Remake(config=)`) moves it, e.g. onto a scratch filesystem with more quota:
`Remake.resolve_config` calls `set_metadata_dir` with the resolved value, so
everything here is evaluated at use time, never frozen at import.

The setting is process-global (like the cwd anchoring). Worker processes get
it by loading the remakefile, which re-reads the config.
"""
import os
from pathlib import Path

DEFAULT_METADATA_DIR = Path('.remake')

_metadata_dir = DEFAULT_METADATA_DIR


def metadata_dir():
    """The current metadata directory: `.remake` (relative to the cwd) unless
    `set_metadata_dir` has pointed it elsewhere (then an absolute path)."""
    return _metadata_dir


def set_metadata_dir(value=None, base=None):
    """Set the metadata directory. `value` None or '' restores the default
    `.remake`. Otherwise `~` and environment variables are expanded, and a
    relative path is taken relative to `base` (the project directory; the
    cwd if None); the stored path is absolute. Returns the new directory."""
    global _metadata_dir
    if not value:
        _metadata_dir = DEFAULT_METADATA_DIR
        return _metadata_dir
    path = Path(os.path.expandvars(os.path.expanduser(str(value))))
    if not path.is_absolute():
        path = Path(base if base is not None else os.getcwd()) / path
    _metadata_dir = Path(os.path.abspath(path))
    return _metadata_dir


def db_path():
    """The default metadata database: `<metadata dir>/remake.db`."""
    return metadata_dir() / 'remake.db'


def results_root():
    """Per-task result sidecars: `<metadata dir>/tasks/results`."""
    return metadata_dir() / 'tasks' / 'results'


def task_log_root():
    """Per-task logs: `<metadata dir>/tasks/log`."""
    return metadata_dir() / 'tasks' / 'log'


def running_root():
    """Running-task markers (multiproc/dask): `<metadata dir>/tasks/running`."""
    return metadata_dir() / 'tasks' / 'running'


def environments_root():
    """Environment fingerprints: `<metadata dir>/environments`."""
    return metadata_dir() / 'environments'


def jobs_dir():
    """SLURM job specs and job-id sidecars: `<metadata dir>/jobs`."""
    return metadata_dir() / 'jobs'


def slurm_dir():
    """Generated sbatch scripts: `<metadata dir>/slurm`."""
    return metadata_dir() / 'slurm'


def slurm_output_dir():
    """SLURM job stdout/stderr: `<metadata dir>/slurm/output`."""
    return slurm_dir() / 'output'


def submit_path():
    """The master SLURM script: `<metadata dir>/submit.sh`."""
    return metadata_dir() / 'submit.sh'


def log_file(name):
    """A remake log file (e.g. `remake.log`) in the metadata directory."""
    return metadata_dir() / name
