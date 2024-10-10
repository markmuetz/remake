from .version import __version__

from .loader import load_remake
from .remake_base import Remake
from .archive import ArchiveV1, ArchiveV1Rule
from .task import Task
from .rule import Rule

# For legacy remakefiles.
from .rule import Rule as TaskRule
