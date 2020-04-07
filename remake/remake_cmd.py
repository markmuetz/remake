import sys
import argparse
import inspect
from logging import getLogger
from pathlib import Path
from typing import List

import numpy as np
from tabulate import tabulate

from remake.setup_logging import setup_stdout_logging
from remake.version import get_version
from remake.util import load_module
from remake.task_control import TaskControl
from remake.metadata import try_json_read

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
    run_parser.add_argument('filenames', nargs='*', default=['remakefile.py'])
    run_parser.add_argument('--force', '-f', action='store_true')
    # run_parser.add_argument('--func', nargs=1, help)
    run_parser.add_argument('--one', '-o', action='store_true')
    run_parser.add_argument('--tasks', '-t', nargs='*')

    file_info_parser = subparsers.add_parser('file-info', help='Information about the given file')
    file_info_parser.add_argument('filenames', nargs='*')
    file_info_parser.add_argument('--remake-dir', '-r', nargs='?')

    task_control_info_parser = subparsers.add_parser('task-control-info',
                                                     help='Information about the given task control')
    task_control_info_parser.add_argument('filenames', nargs='*')
    task_control_info_parser.add_argument('--format', '-f', default='medium', choices=['short', 'medium', 'long'])

    task_info_parser = subparsers.add_parser('task-info', help='Information about the given task')
    task_info_parser.add_argument('--task', nargs=1)
    task_info_parser.add_argument('filename', nargs=1)
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
        remake_run(args.filenames, args.force, args.one, args.tasks)
    elif args.subcmd_name == 'version':
        print(get_version(form='long' if args.long else 'short'))
    elif args.subcmd_name == 'file-info':
        file_info(args.remake_dir, args.filenames)
    elif args.subcmd_name == 'task-control-info':
        task_control_info(args.filenames, args.format)
    elif args.subcmd_name == 'task-info':
        task_info(args.filename[0], args.format, args.task[0])
    else:
        assert False, f'Subcommand {args.subcmd_name} not recognized'


def file_info(remake_dir, filenames):
    # Very rough, but it demonstrates how to get information from a path, going through the medatadata dir.
    remake_dir = Path(remake_dir).absolute()
    dotremake_dir = remake_dir / '.remake'
    file_metadata_dir = dotremake_dir / 'metadata_v4' / 'file_metadata'
    for path in (Path(fn).absolute() for fn in filenames):
        if not path.exists():
            raise Exception(f'No file: {path}')
        file_metadata_path = file_metadata_dir.joinpath(*path.parent.parts[1:]) / (path.name + '.metadata')
        if not file_metadata_path.exists():
            print(f'No metadata for {path}')
            continue
        print(file_metadata_path.read_text())
        file_metadata = try_json_read(file_metadata_path)
        remake_task_ctrl_path = remake_dir / (file_metadata['task_control_name'] + '.py')
        task_ctrl_module = load_module(remake_task_ctrl_path)
        task_ctrl = _load_task_ctrls(remake_task_ctrl_path, task_ctrl_module)[0]
        task_ctrl.build_task_DAG()
        path_md = task_ctrl.metadata_manager.path_metadata_map[path]
        task = task_ctrl.output_task_map[path]
        print(path_md)
        print(task)


def task_info(filename, output_format, task_path_hash_key):
    task_ctrl_module = load_module(filename)
    task_ctrl = _load_task_ctrls(filename, task_ctrl_module)[0]
    task_ctrl.finalize()
    task = task_ctrl.task_from_path_hash_key[task_path_hash_key]
    print(repr(task))


def task_control_info(filenames, output_format='medium'):
    if output_format == 'short':
        rows = []
    for filename in filenames:
        task_ctrl_module = load_module(filename)
        task_ctrl = _load_task_ctrls(filename, task_ctrl_module)[0]
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
                task_status = ''
                if task in task_ctrl.completed_tasks:
                    task_status = 'completed'
                elif task in task_ctrl.pending_tasks:
                    task_status = 'pending  '
                elif task in task_ctrl.remaining_tasks:
                    task_status = 'remaining'
                print(f'{i + 1}/{len(task_ctrl.tasks)}, {task_status}: {task.path_hash_key()} {task.short_str()}')

    if output_format == 'short':
        totals = list(np.array([r[1:] for r in rows]).sum(axis=0))
        rows.append(['Total'] + totals)
        print(tabulate(rows, headers=('Name', 'completed', 'pending', 'remaining', 'total')))


def remake_run(filenames, force, one, tasks):
    task_ctrls = []
    if len(filenames) > 1:
        for filename in filenames:
            task_ctrl_module = load_module(filename)
            loaded_task_ctrls = _load_task_ctrls(filename, task_ctrl_module)
            logger.debug(f'created TaskControls: {loaded_task_ctrls}')
            task_ctrls.extend(loaded_task_ctrls)
        # Naive -- need to add something like add_task_ctrl()
        # otherwise will get wrong filename as here.
        # uber_task_ctrl = TaskControl(__file__)
        # for filename in filenames:
        #     task_ctrl_module = load_module(filename)
        #     task_ctrl = _load_task_ctrl(filename, task_ctrl_module)
        #     logger.debug(f'created TaskControl: {task_ctrl}')
        #     for task in task_ctrl.tasks:
        #         uber_task_ctrl.add(task)
    elif len(filenames) == 1:
        task_ctrl_module = load_module(filenames[0])
        loaded_task_ctrls = _load_task_ctrls(filenames[0], task_ctrl_module)
        logger.debug(f'created TaskControls: {loaded_task_ctrls}')
        task_ctrls.extend(loaded_task_ctrls)
    else:
        assert False, 'Should be one or more filenames'

    for task_ctrl in task_ctrls:
        task_ctrl.finalize()
        if not task_ctrl.pending_tasks and not force:
            print(f'{task_ctrl.name}: {len(task_ctrl.completed_tasks)} tasks already run')
        if not tasks:
            if one:
                task_ctrl.run_one(force=force)
            else:
                task_ctrl.run(force=force)
        else:
            for task_hash_key in tasks:
                task = task_ctrl.task_from_path_hash_key[task_hash_key]
                task_ctrl.running_tasks.append(task)
                task_ctrl.run_task(task, force)
                task_ctrl.task_complete(task)


def _load_task_ctrls(filename, task_ctrl_module):
    task_ctrls = []
    functions = [o for o in [getattr(task_ctrl_module, m) for m in dir(task_ctrl_module)]
                 if inspect.isfunction(o)]
    for func in functions:
        if hasattr(func, 'is_remake_task_control') and func.is_remake_task_control:
            task_ctrl = func()
            if not isinstance(task_ctrl, TaskControl):
                raise Exception(f'{task_ctrl} is not a TaskControl (defined in {func})')
            task_ctrls.append(task_ctrl)
    if not task_ctrls:
        raise Exception(f'No task controls defined in {filename}')

    return task_ctrls

    if not hasattr(task_ctrl_module, 'REMAKE_TASK_CTRL_FUNC'):
        raise Exception(f'No REMAKE_TASK_CTRL_FUNC defined in {filename}')
    task_ctrl_func_name = task_ctrl_module.REMAKE_TASK_CTRL_FUNC
    if not hasattr(task_ctrl_module, task_ctrl_func_name):
        raise Exception(f'No function {task_ctrl_func_name} defined in {filename}')
    task_ctrl_func = getattr(task_ctrl_module, task_ctrl_func_name)
    logger.debug(f'got task_ctrl_func: {task_ctrl_func}')
    task_ctrl = task_ctrl_func()
    if not isinstance(task_ctrl, TaskControl):
        raise Exception(f'{task_ctrl} is not a TaskControl')
    return task_ctrl
