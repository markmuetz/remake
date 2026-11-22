from .version import __version__

from .core import (
    Defer,
    Dir,
    Ensure,
    FileToken,
    OutputToken,
    PathToken,
    Remake,
    RemakeError,
    Rule,
    S3Object,
    ScopeError,
    ScopeWarning,
    SignatureError,
    Task,
    ZarrStore,
    deferrable,
    rule,
)
from .executors import (
    DaskExecutor,
    Executor,
    MultiprocExecutor,
    SingleprocExecutor,
    SlurmExecutor,
)
from .loader import load_remake
from .metadata import MetadataManager, Sqlite3Backend, TaskRecord

# The declared public API. Everything else (remake.core.*, remake.util.*) is
# internal. tests/unit/test_public_api.py checks this against docs/api/.
__all__ = [
    'DaskExecutor',
    'Defer',
    'Dir',
    'Ensure',
    'Executor',
    'FileToken',
    'MetadataManager',
    'MultiprocExecutor',
    'OutputToken',
    'PathToken',
    'Remake',
    'RemakeError',
    'Rule',
    'S3Object',
    'ScopeError',
    'ScopeWarning',
    'SignatureError',
    'SingleprocExecutor',
    'SlurmExecutor',
    'Sqlite3Backend',
    'Task',
    'TaskRecord',
    'ZarrStore',
    'deferrable',
    'load_remake',
    'rule',
]
