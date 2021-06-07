import sys
import importlib.util
import hashlib
from logging import getLogger
from pathlib import Path, PosixPath  # noqa: F401
from typing import Union
import subprocess as sp

logger = getLogger(__name__)

# BUF_SIZE is totally arbitrary, change for your app!
SHA1_BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


def tmp_to_actual_path(path: Path) -> Path:
    """Convert a temporary remake path to an actual path.

    When writing to an output path, remake uses a temporary path then copies to the actual path on completion.
    This function can be used to see the actual path from the temporary path.

    >>> tmp_to_actual_path(Path('.remake.tmp.output.txt'))
    PosixPath('output.txt')

    :param path: temporary remake path
    :return: actual path
    """
    if not path.name[:12] == '.remake.tmp.':
        raise ValueError(f'Path must be a remake tmp path (start with ".remake.tmp."): {path}')

    return path.parent / path.name[12:]


def sha1sum(path: Path, buf_size: int = SHA1_BUF_SIZE) -> str:
    """Calculate sha1 sum for a path.

    >>> sha1sum(Path('examples/data/in.txt'))
    '3620f0704e803d65098e5f2b836633b166e25474'

    :param path: file path to calculate sha1 sum for
    :param buf_size: buffer size for reading file
    :return: sha1 sum of input path
    """
    logger.debug(f'calc sha1sum for {path}')
    sha1 = hashlib.sha1()
    with path.open('rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def load_module(local_filename: Union[str, Path]):
    """Use Python internals to load a Python module from a filename.

    >>> load_module('examples/ex1.py').__name__
    'ex1'

    :param local_filename: name of module to load
    :return: module
    """
    module_path = Path.cwd() / local_filename
    if not module_path.exists():
        raise Exception(f'Module file {module_path} does not exist')

    # No longer needed due to sys.modules line below.
    # Make sure any local imports in the module script work.
    sys.path.append(str(module_path.parent))
    module_name = Path(local_filename).stem

    try:
        # See: https://stackoverflow.com/a/50395128/54557
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except SyntaxError:
        print(f'Bad syntax in module file {module_path}')
        raise

    return module


def format_path(path: Union[Path, str], **kwargs) -> Path:
    """Format a path based on `**kwargs`.

    >>> format_path(Path('some/path/{dirname}/{filename}'), dirname='output', filename='out.txt')
    PosixPath('some/path/output/out.txt')

    :param path: path with python format-style braces
    :param kwargs: keyword args to substitute
    :return: formatted path
    """
    return Path(str(path).format(**kwargs))


def sysrun(cmd):
    """Run a system command, returns a CompletedProcess

    >>> print(sysrun('echo "hello"').stdout)
    hello
    <BLANKLINE>

    raises CalledProcessError if cmd is bad.
    to access output: sysrun(cmd).stdout"""
    return sp.run(cmd, check=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')
