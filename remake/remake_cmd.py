import sys
import argparse
from logging import getLogger
from pathlib import Path
from typing import List

import numpy as np
from tabulate import tabulate

from remake.setup_logging import setup_stdout_logging
from remake.version import get_version
from remake.load_task_ctrls import load_task_ctrls
from remake.remake_base import Remake

logger = getLogger(__name__)


def exception_info(ex_type, value, tb):
    try:
        # Might not be installed.
        import ipdb as debug
    except ImportError:
        import pdb as debug

    import traceback
    traceback.print_exception(ex_type, value, tb)
    debug.pm()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='remake command line tool')

    # Top-level arguments.
    parser.add_argument('--debug', '-D', help='Enable debug logging', action='store_true')
    parser.add_argument('--debug-exception', '-X', help='Launch ipdb on exception', action='store_true')

    subparsers = parser.add_subparsers(dest='subcmd_name', required=True)
    # name of subparser ends up in subcmd_name -- use for command dispatch.

    # TODO: API is different for each command!
    run_parser = subparsers.add_parser('run', help='Run remake')
    run_parser.add_argument('remakefiles', nargs='*', default=['remakefile.py'])
    run_parser.add_argument('--force', '-f', action='store_true')
    # run_parser.add_argument('--func', nargs=1, help)
    run_parser.add_argument('--one', '-o', action='store_true')
    run_parser.add_argument('--reasons', '-R', action='store_true')
    run_parser.add_argument('--tasks', '-t', nargs='*')

    file_info_parser = subparsers.add_parser('file-info', help='Information about file')
    file_info_parser.add_argument('remakefile')
    file_info_parser.add_argument('filenames', nargs='*')

    task_control_info_parser = subparsers.add_parser('remakefile-info',
                                                     help='Information about remakefile')
    task_control_info_parser.add_argument('remakefiles', nargs='*')
    task_control_info_parser.add_argument('--format', '-f', default='medium', choices=['short', 'medium', 'long'])

    task_info_parser = subparsers.add_parser('task-info', help='Information about task')
    task_info_parser.add_argument('--task', nargs=1)
    task_info_parser.add_argument('remakefile', nargs=1)
    task_info_parser.add_argument('--format', '-f', default='medium', choices=['short', 'medium', 'long'])

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
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info

    # Dispatch command.
    # N.B. args should always be dereferenced at this point,
    # not passed into any subsequent functions.
    if args.subcmd_name == 'run':
        remake_run(args.remakefiles, args.force, args.one, args.tasks, args.reasons)
    elif args.subcmd_name == 'version':
        print(get_version(form='long' if args.long else 'short'))
    elif args.subcmd_name == 'file-info':
        file_info(args.remakefile, args.filenames)
    elif args.subcmd_name == 'remakefile-info':
        remakefile_info(args.remakefiles, args.format)
    elif args.subcmd_name == 'task-info':
        task_info(args.remakefile[0], args.format, args.task[0])
    else:
        assert False, f'Subcommand {args.subcmd_name} not recognized'


def file_info(remakefile, filenames):
    loaded_task_ctrl = load_task_ctrls(remakefile)[0]
    Remake.finalize()

    for path in (Path(fn).absolute() for fn in filenames):
        if not path.exists():
            raise Exception(f'No file: {path}')
        print(path)
        path_md, used_by_tasks, produced_by_task = Remake.file_info(path)
        if not path_md:
            print(f'Path not found in {Remake.task_ctrl.name}')
            print()
            continue
        changed, needs_write = path_md.compare_path_with_previous()
        if changed:
            print('Path contents has changed since last use')
        else:
            print('Path contents unchanged')
        if needs_write:
            print('Path metadata is out of date')
        else:
            print('Path metadata is up to date')
        if produced_by_task:
            print('Produced by:')
            print('  ' + str(produced_by_task))
        if used_by_tasks:
            print('Used by:')
            for task in used_by_tasks:
                print('  ' + str(task))
        print()


def task_info(remakefile, output_format, task_path_hash_key):
    task_ctrl = load_task_ctrls(remakefile)[0]
    task_ctrl.finalize()
    task = task_ctrl.task_from_path_hash_key[task_path_hash_key]
    print(repr(task))
    task_md = task_ctrl.metadata_manager.task_metadata_map[task]
    print(task_md.task_requires_rerun())


def remakefile_info(remakefiles, output_format='medium'):
    if output_format == 'short':
        rows = []
    for remakefile in remakefiles:
        task_ctrl = load_task_ctrls(remakefile)[0]
        task_ctrl.finalize()
        if output_format == 'short':
            rows.append([task_ctrl.name,
                         len(task_ctrl.completed_tasks),
                         len(task_ctrl.pending_tasks),
                         len(task_ctrl.remaining_tasks),
                         len(task_ctrl.tasks),
                         ])
        elif output_format == 'medium':
            task_ctrl.print_status()
        elif output_format == 'long':
            print(f'{task_ctrl.name}')
            for i, task in enumerate(task_ctrl.sorted_tasks):
                task_status = task_ctrl.statuses.task_status(task)
                print(f'{i + 1}/{len(task_ctrl.tasks)}, {task_status}: {task.path_hash_key()} {task.short_str()}')

    if output_format == 'short':
        totals = list(np.array([r[1:] for r in rows]).sum(axis=0))
        rows.append(['Total'] + totals)
        print(tabulate(rows, headers=('Name', 'completed', 'pending', 'remaining', 'total')))


def remake_run(remakefiles, force, one, task_hash_keys, print_reasons):
    task_ctrls = []
    if len(remakefiles) > 1:
        for remakefile in remakefiles:
            loaded_task_ctrls = load_task_ctrls(remakefile)
            logger.debug(f'created TaskControls: {loaded_task_ctrls}')
            task_ctrls.extend(loaded_task_ctrls)
        # Naive -- need to add something like add_task_ctrl()
        # otherwise will get wrong remakefile as here.
        # uber_task_ctrl = TaskControl(__file__)
        # for remakefile in remakefiles:
        #     task_ctrl = load_task_ctrl(remakefile)
        #     logger.debug(f'created TaskControl: {task_ctrl}')
        #     for task in task_ctrl.tasks:
        #         uber_task_ctrl.add(task)
    elif len(remakefiles) == 1:
        loaded_task_ctrls = load_task_ctrls(remakefiles[0])
        logger.debug(f'created TaskControls: {loaded_task_ctrls}')
        task_ctrls.extend(loaded_task_ctrls)
    else:
        assert False, 'Should be one or more remakefiles'

    for task_ctrl in task_ctrls:
        if not task_ctrl.finalized:
            task_ctrl.finalize()
        task_ctrl.print_reasons = print_reasons
        if not task_ctrl.pending_tasks and not force:
            print(f'{task_ctrl.name}: {len(task_ctrl.completed_tasks)} tasks already run')
        if not task_hash_keys:
            if one:
                task_ctrl.run_one(force=force)
            else:
                task_ctrl.run(force=force)
        else:
            tasks = [task_ctrl.task_from_path_hash_key[t] for t in task_hash_keys]
            task_ctrl.run(requested_tasks=tasks, force=force)
