import subprocess as sp
import sys
from io import StringIO
from pathlib import Path
from typing import Union


def sysrun(cmd):
    """Run a system command, returns a CompletedProcess

    >>> print(sysrun('echo "hello"').stdout)
    hello
    <BLANKLINE>

    raises CalledProcessError if cmd is bad.
    to access output: sysrun(cmd).stdout"""
    return sp.run(cmd, check=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')


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


def task_log_path(task):
    """Per-task log file, named by stable task key (sharded: 256 buckets per
    rule, see design_docs/per_task_logging.md)."""
    return Path('.remake/tasks/log') / task.rule.name / task.key[:2] / f'{task.key[2:]}.log'
