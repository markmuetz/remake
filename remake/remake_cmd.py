import sys
import argparse
from logging import getLogger
from typing import List

from remake.setup_logging import setup_stdout_logging
from remake.util import load_module

logger = getLogger(__name__)


def exception_info(ex_type, value, tb):
    import ipdb
    import traceback
    traceback.print_exception(ex_type, value, tb)
    ipdb.pm()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='remake command line tool')

    # Top-level arguments.
    parser.add_argument('--debug', '-D', help='Enable debug logging', action='store_true')
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
    setup_stdout_logging(loglevel)

    if args.debug_exception:
        sys.excepthook = exception_info

    # Dispatch command.
    # N.B. args should always be dereferenced at this point,
    # not passed into any subsequent functions.
    if args.subcmd_name == 'run':
        remake_run(args.filename)
    elif args.subcmd_name == 'version':
        print(0.3)


def remake_run(filename):
    task_ctrl_module = load_module(filename)
    if not hasattr(task_ctrl_module, 'REMAKE_TASK_CTRL_FUNC'):
        raise Exception(f'No REMAKE_TASK_CTRL_FUNC defined in {filename}')

    task_ctrl_func_name = task_ctrl_module.REMAKE_TASK_CTRL_FUNC
    if not hasattr(task_ctrl_module, task_ctrl_func_name):
        raise Exception(f'No function {task_ctrl_func_name} defined in {filename}')

    task_ctrl_func = getattr(task_ctrl_module, task_ctrl_func_name)
    logger.debug(f'got task_ctrl_func: {task_ctrl_func}')
    task_ctrl = task_ctrl_func()
    task_ctrl.finalize()
    if not task_ctrl.pending_tasks:
        print('No tasks to run')
    else:
        task_ctrl.run()

