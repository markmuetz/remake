import sys
import argparse
from typing import List


def exception_info(ex_type, value, tb):
    import ipdb
    import traceback
    traceback.print_exception(ex_type, value, tb)
    ipdb.pm()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='remake command line tool')

    # Top-level arguments.
    parser.add_argument('--debug', '-D', help='Enable debug logging', action='store_true')
    if not sys.platform.startswith('win'):
        parser.add_argument('--bw', '-B', help='Disable colour logging', action='store_true')
    parser.add_argument('--debug-exception', '-X', help='Launch ipdb on exception', action='store_true')

    subparsers = parser.add_subparsers(dest='subcmd_name', required=True)
    # name of subparser ends up in subcmd_name -- use for command dispatch.

    run_parser = subparsers.add_parser('run', help='Run remake')
    run_parser.add_argument('filename', nargs='?')

    # version
    version_parser = subparsers.add_parser('version', help='Print remake version')
    version_parser.add_argument('--long', '-l', action='store_true', help='long version')

    return parser


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = _build_parser()
    args = parser.parse_args(argv[1:])
    return args


def remake_cmd(argv: List[str] = sys.argv) -> None:
    args = _parse_args(argv)
    loglevel = 'DEBUG' if args.debug else 'INFO'

    if sys.platform.startswith('win'):
        args.bw = True

    if args.debug_exception:
        sys.excepthook = exception_info

    # Dispatch command.
    # N.B. args should always be dereferenced at this point,
    # not passed into any subsequent functions.
    if args.subcmd_name == 'run':
        print('run')
        print(args.filename)
    elif args.subcmd_name == 'version':
        print(0.3)
        raise Exception()
