from .version import __version__

from .remake_cmd import remake_cmd
from .loader import load_remake, load_archive
from .core import Remake, Rule, Task, ArchiveV1, ArchiveV1Rule

# For legacy remakefiles.
from .core import Rule as TaskRule
