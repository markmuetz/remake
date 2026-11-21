"""`python -m remake ...` runs the remake CLI (used by `run-all`, which runs
each remakefile in its own process)."""
import sys

from .remake_cmd import remake_cmd

if __name__ == '__main__':
    sys.exit(remake_cmd(sys.argv))
