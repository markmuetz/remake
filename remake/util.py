import sys
import importlib.util
import hashlib
from logging import getLogger
from pathlib import Path
from typing import Union
import subprocess as sp

logger = getLogger(__name__)

# BUF_SIZE is totally arbitrary, change for your app!
SHA1_BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


def tmp_to_actual_path(path: Path) -> Path:
    if not path.name[:12] == '.remake.tmp.':
        raise ValueError(f'Path must be a remake tmp path (start with ".remake.tmp."): {path}')

    return path.parent / path.name[12:]


def sha1sum(path: Path, buf_size: int = SHA1_BUF_SIZE) -> str:
    logger.debug(f'calc sha1sum for {path}')
    sha1 = hashlib.sha1()
    with path.open('rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def load_module(local_filename):
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
    except SyntaxError as se:
        print(f'Bad syntax in module file {module_path}')
        raise

    return module


def fmtp(path: Union[Path, str], **kwargs) -> Path:
    """Format a Path"""
    return Path(str(path).format(**kwargs))


def sysrun(cmd):
    """Run a system command, returns a CompletedProcess

    raises CalledProcessError if cmd is bad.
    to access output: sysrun(cmd).stdout"""
    return sp.run(cmd, check=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')
