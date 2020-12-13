import os
import sys
import argparse
from logging import getLogger
from pathlib import Path
from time import sleep
from typing import List
try:
    # Might not be installed.
    import ipdb as debug
except ImportError:
    import pdb as debug

from tabulate import tabulate

from remake.setup_logging import setup_stdout_logging
from remake.version import get_version
from remake.load_task_ctrls import load_remake
from remake.task_query_set import TaskQuerySet

logger = getLogger(__name__)


def exception_info(ex_type, value, tb):
    import traceback
    traceback.print_exception(ex_type, value, tb)
    debug.pm()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='remake command line tool')
    parser._actions[0].help = 'Show this help message and exit'

    # Top-level arguments.
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--debug', '-D', help='Enable debug logging', action='store_true')
    group.add_argument('--info', '-I', help='Enable info logging', action='store_true')
    group.add_argument('--warning', '-W', help='Warning logging only', action='store_true')
    parser.add_argument('--debug-exception', '-X', help=f'Launch {debug.__name__} on exception', action='store_true')
    parser.add_argument('--no-colour', '-B', help='Black and white logging', action='store_true')

    subparsers = parser.add_subparsers(dest='subcmd_name', required=True)
    # name of subparser ends up in subcmd_name -- use for command dispatch.

    # TODO: API is different for each command!
    run_parser = subparsers.add_parser('run', help='Run remake')
    run_parser.add_argument('remakefiles', nargs='*', default=['remakefile.py'])
    run_parser.add_argument('--force', '-f', action='store_true')
    run_parser.add_argument('--one', '-o', action='store_true')
    run_parser.add_argument('--reasons', '-R', action='store_true')
    run_parser.add_argument('--executor', '-E', default='singleproc')
    run_parser.add_argument('--display', '-d', choices=['print_status', 'task_dag'])

    run_tasks_parser = subparsers.add_parser('run-task', help='Run remake')
    run_tasks_parser.add_argument('remakefile', default='remakefile.py')
    run_tasks_parser.add_argument('--force', '-f', action='store_true')
    run_tasks_parser.add_argument('--reasons', '-R', action='store_true')
    run_tasks_parser.add_argument('--executor', '-E', default='singleproc')
    run_tasks_parser.add_argument('--display', '-d', choices=['print_status', 'task_dag'])
    run_tasks_parser.add_argument('--tasks', '-t', nargs='*')
    # TODO: Add e.g. --filter, --rule in a smart way.

    file_info_parser = subparsers.add_parser('file-info', help='Information about file')
    file_info_parser.add_argument('remakefile')
    file_info_parser.add_argument('filenames', nargs='*')

    remakefile_info_parser = subparsers.add_parser('remakefile-info',
                                                   help='Information about remakefile')
    remakefile_info_parser.add_argument('remakefiles', nargs='*')
    remakefile_info_parser.add_argument('--format', '-f', default='medium', choices=['short', 'medium', 'long'])

    task_info_parser = subparsers.add_parser('task-info', help='Information about task')
    task_info_parser.add_argument('--task', nargs=1)
    task_info_parser.add_argument('remakefile', nargs=1)
    task_info_parser.add_argument('--format', '-f', default='medium', choices=['short', 'medium', 'long'])

    ls_tasks_parser = subparsers.add_parser('ls-tasks', help='List tasks')
    ls_tasks_parser.add_argument('--filter', '-F', default=None)
    ls_tasks_parser.add_argument('--rule', '-R', default=None)
    ls_tasks_parser.add_argument('remakefile')

    ls_files_parser = subparsers.add_parser('ls-files', help='List files')
    group = ls_files_parser.add_mutually_exclusive_group()
    group.add_argument('--input', action='store_true')
    group.add_argument('--output', action='store_true')
    group.add_argument('--input-only', action='store_true')
    group.add_argument('--output-only', action='store_true')
    group.add_argument('--inout', action='store_true')
    ls_files_parser.add_argument('--produced-by', action='store_true')
    ls_files_parser.add_argument('--used-by', action='store_true')
    ls_files_parser.add_argument('--exists', action='store_true')
    ls_files_parser.add_argument('remakefile')

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

    if args.debug_exception:
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info

    loglevel = os.getenv('REMAKE_LOGLEVEL', None)
    if loglevel is None:
        if args.debug:
            loglevel = 'DEBUG'
        elif args.info:
            loglevel = 'INFO'
        elif args.warning:
            loglevel = 'WARNING'
        else:
            # Do not output full info logging for -info commands. (Ironic?)
            # Do not output full info logging for ls- commands.
            if args.subcmd_name.endswith('-info') or args.subcmd_name.startswith('ls-'):
                loglevel = 'WARNING'
            else:
                loglevel = 'INFO'
    colour = not args.no_colour
    setup_stdout_logging(loglevel, colour=colour)

    # Dispatch command.
    # N.B. args should always be dereferenced at this point,
    # not passed into any subsequent functions.
    if args.subcmd_name == 'run':
        remake_run(args.remakefiles, args.force, args.one, args.tasks, args.reasons, args.executor, args.display)
    elif args.subcmd_name == 'version':
        print(get_version(form='long' if args.long else 'short'))
    elif args.subcmd_name == 'file-info':
        file_info(args.remakefile, args.filenames)
    elif args.subcmd_name == 'remakefile-info':
        remakefile_info(args.remakefiles, args.format)
    elif args.subcmd_name == 'task-info':
        task_info(args.remakefile[0], args.format, args.task[0])
    elif args.subcmd_name == 'ls-files':
        if args.input:
            filetype = 'input'
        elif args.output:
            filetype = 'output'
        elif args.input_only:
            filetype = 'input_only'
        elif args.output_only:
            filetype = 'output_only'
        elif args.inout:
            filetype = 'inout'
        else:
            filetype = None

        ls_files(args.remakefile, filetype, args.exists)
    elif args.subcmd_name == 'ls-tasks':
        ls_tasks(args.remakefile, args.filter, args.rule)
    else:
        assert False, f'Subcommand {args.subcmd_name} not recognized'


def file_info(remakefile, filenames):
    remake = load_remake(remakefile)
    remake.finalize()

    for path in (Path(fn).absolute() for fn in filenames):
        if path.exists():
            print(f'exists: {path}')
        else:
            print(f'does not exist: {path}')
        path_md, used_by_tasks, produced_by_task = remake.file_info(path)
        if not path_md:
            print(f'Path not found in {remake.task_ctrl.name}')
            print()
            continue
        if produced_by_task:
            print('Produced by:')
            print('  ' + str(produced_by_task))
        if used_by_tasks:
            print('Used by:')
            for task in used_by_tasks:
                print('  ' + str(task))
        if path.exists():
            metadata_has_changed = path_md.compare_path_with_previous()
            if metadata_has_changed:
                print('Path metadata has changed since last use')
            else:
                print('Path metadata unchanged')
        print()


def task_info(remakefile, output_format, task_path_hash_key):
    remake = load_remake(remakefile)
    remake.finalize()
    task = remake.task_ctrl.task_from_path_hash_key[task_path_hash_key]
    print(repr(task))
    task_md = remake.task_ctrl.metadata_manager.task_metadata_map[task]
    print(task_md.task_requires_rerun())


def ls_files(remakefile, filetype=None, exists=False):
    remake = load_remake(remakefile)
    if filetype is None:
        files = sorted(set(remake.task_ctrl.input_task_map.keys()) | set(remake.task_ctrl.output_task_map.keys()))
    elif filetype == 'input':
        files = sorted(remake.task_ctrl.input_task_map.keys())
    elif filetype == 'output':
        files = sorted(remake.task_ctrl.output_task_map.keys())
    elif filetype == 'input_only':
        files = sorted(set(remake.task_ctrl.input_task_map.keys()) - set(remake.task_ctrl.output_task_map.keys()))
    elif filetype == 'output_only':
        files = sorted(set(remake.task_ctrl.output_task_map.keys()) - set(remake.task_ctrl.input_task_map.keys()))
    elif filetype == 'inout':
        files = sorted(set(remake.task_ctrl.output_task_map.keys()) & set(remake.task_ctrl.input_task_map.keys()))
    else:
        raise Exception(f'Unknown {filetype=}')
    if exists:
        files = [f for f in files if f.exists()]
    for file in files:
        print(file)


def ls_tasks(remakefile, tfilter, rule):
    remake = load_remake(remakefile)
    tasks = TaskQuerySet([t for t in remake.tasks], remake.task_ctrl)
    if tfilter:
        filter_kwargs = dict([kv.split('=') for kv in tfilter.split(',')])
        tasks = tasks.filter(cast_to_str=True, **filter_kwargs)
    if rule:
        tasks = tasks.in_rule(rule)
    for task in tasks:
        print(task)



def remakefile_info(remakefiles, output_format='medium'):
    if output_format == 'short':
        rows = []
    for remakefile in remakefiles:
        remake = load_remake(remakefile)
        remake.finalize()
        if output_format == 'short':
            rows.append([remake.task_ctrl.name,
                         len(remake.task_ctrl.completed_tasks),
                         len(remake.task_ctrl.pending_tasks),
                         len(remake.task_ctrl.remaining_tasks),
                         len(remake.task_ctrl.tasks),
                         ])
        elif output_format == 'medium':
            remake.task_ctrl.print_status()
        elif output_format == 'long':
            print(f'{remake.task_ctrl.name}')
            for i, task in enumerate(remake.task_ctrl.sorted_tasks):
                task_status = remake.task_ctrl.statuses.task_status(task)
                print(f'{i + 1}/{len(remake.task_ctrl.tasks)}, {task_status}: {task.path_hash_key()} {task.short_str()}')

    if output_format == 'short':
        # totals = list(np.array([r[1:] for r in rows]).sum(axis=0))
        # Same thing without numpy.
        totals = [sum(col) for col in list(zip(*rows))[1:]]
        rows.append(['Total'] + totals)
        print(tabulate(rows, headers=('Name', 'completed', 'pending', 'remaining', 'total')))


def remake_run(remakefiles, force, one, task_hash_keys, print_reasons, executor, display):
    remakes = []
    for remakefile in remakefiles:
        remakes.append(load_remake(remakefile))

    for remake in remakes:
        if not remake.finalized:
            remake.finalize()
        remake.task_ctrl.print_reasons = print_reasons
        remake.task_ctrl.set_executor(executor)
        if display == 'print_status':
            remake.task_ctrl.display_func = remake.task_ctrl.__class__.print_status
        elif display == 'task_dag':
            from remake.experimental.networkx_displays import display_task_status
            remake.task_ctrl.display_func = display_task_status
        elif display:
            raise Exception(f'display {display} not recognized')
        if (not remake.task_ctrl.rescan_tasks) and (not remake.task_ctrl.pending_tasks) and (not force):
            logger.info(f'{remake.task_ctrl.name}: {len(remake.task_ctrl.completed_tasks)} tasks already run')
        if not task_hash_keys:
            if one:
                remake.run_one(force=force)
            else:
                remake.run_all(force=force)
        else:
            tasks = [remake.task_ctrl.task_from_path_hash_key[t] for t in task_hash_keys]
            remake.run_requested(requested_tasks=tasks, force=force)
        if display == 'task_dag':
            # Give user time to see final task_dag state.
            sleep(3)
