import os
import sys
import subprocess as sp
from dataclasses import dataclass
from io import StringIO
from pathlib import Path, PosixPath
from typing import Union


def sysrun(cmd):
    """Run a system command, returns a CompletedProcess

    >>> print(sysrun('echo "hello"').stdout)
    hello
    <BLANKLINE>

    raises CalledProcessError if cmd is bad.
    to access output: sysrun(cmd).stdout"""
    return sp.run(cmd, check=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')


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


def format_path(path: Union[Path, str], **kwargs) -> Path:
    """Format a path based on `**kwargs`.

    >>> format_path(Path('some/path/{dirname}/{filename}'), dirname='output', filename='out.txt')
    PosixPath('some/path/output/out.txt')

    :param path: path with python format-style braces
    :param kwargs: keyword args to substitute
    :return: formatted path
    """
    return Path(str(path).format(**kwargs))


class Capturing(list):
    """Capture stdout from function.

    https://stackoverflow.com/a/16571630/54557
    """

    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


@dataclass
class GitInfo:
    loc: Path
    is_repo: bool
    git_hash: str
    describe: str
    status: str


def get_git_info(location='.'):
    cwd = os.getcwd()
    os.chdir(location)
    try:
        # Will raise sp.CalledProcessError if not in git repo.
        git_hash = sysrun('git rev-parse HEAD').stdout.strip()
        git_describe = sysrun('git describe --tags --always').stdout.strip()
        if sysrun('git status --porcelain').stdout == '':
            return GitInfo(location, True, git_hash, git_describe, 'clean')
        else:
            return GitInfo(location, True, git_hash, git_describe, 'uncommitted_changes')
    except sp.CalledProcessError as ex:
        return GitInfo(location, False, None, None, 'not_a_repo')
    finally:
        os.chdir(cwd)


def git_archive(name, commitish, archive_path):
    archive_path.parent.mkdir(exist_ok=True, parents=True)
    sysrun(f'git archive --format tar.gz {commitish} -o {archive_path}')

    return archive_path
