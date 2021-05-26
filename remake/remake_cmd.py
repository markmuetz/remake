import os
import sys
import argparse
from logging import getLogger
from time import sleep
from typing import List, Union, Optional, Sequence, Text
try:
    # Might not be installed.
    import ipdb as debug
except ImportError:
    import pdb as debug

try:
    import argcomplete
except ImportError:
    argcomplete = None
from tabulate import tabulate

from remake.setup_logging import setup_stdout_logging
from remake.version import get_version
from remake.load_remake import load_remake
from remake.remake_exceptions import RemakeError
from remake.bcolors import bcolors
from remake.monitor import remake_curses_monitor

logger = getLogger(__name__)


def log_error(ex_type, value, tb):
    if isinstance(value, RemakeError):
        logger.error(value)
    else:
        import traceback
        traceback.print_exception(ex_type, value, tb)


def exception_info(ex_type, value, tb):
    import traceback
    traceback.print_exception(ex_type, value, tb)
    debug.pm()


class Arg:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return self.args, self.kwargs

    def __str__(self):
        return f'Arg({self.args}, {self.kwargs})'

    def __repr__(self):
        return str(self)


class MutuallyExclusiveGroup:
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        argstr = '\n  '.join(str(a) for a in self.args)
        return f'MutuallyExclusiveGroup(\n  {argstr})'

    def __repr__(self):
        return str(self)


def add_argset(parser, argset):
    if isinstance(argset, MutuallyExclusiveGroup):
        group = parser.add_mutually_exclusive_group()
        for arg in argset.args:
            group.add_argument(*arg.args, **arg.kwargs)
    elif isinstance(argset, Arg):
        parser.add_argument(*argset.args, **argset.kwargs)
    else:
        raise Exception(f'Unrecognized argset type {argset}')


class RemakeParser:
    args = [
        MutuallyExclusiveGroup(
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true'),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
        Arg('--debug-exception', '-X', help=f'Launch {debug.__name__} on exception',
            action='store_true'),
        Arg('--no-colour', '-B', help='Black and white logging', action='store_true'),
    ]
    run_ctrl_group = [
        Arg('--force', '-f', action='store_true'),
        Arg('--reasons', '-r', action='store_true'),
        Arg('--executor', '-E', default='singleproc'),
        Arg('--display', '-d', choices=['print_status', 'task_dag']),
    ]
    task_filter_group = [
        Arg('--filter'),
        Arg('--rule'),
        Arg('--requires-rerun', '-R', action='store_true'),
        Arg('--uses-file', '-U'),
        Arg('--produces-file', '-P'),
        Arg('--ancestor-of', '-A', help='includes requested task'),
        Arg('--descendant-of', '-D', help='includes requested task'),
    ]
    ls_files_group = [
        MutuallyExclusiveGroup(
            Arg('--input', action='store_true'),
            Arg('--output', action='store_true'),
            Arg('--input-only', action='store_true'),
            Arg('--output-only', action='store_true'),
            Arg('--inout', action='store_true'),
        ),
        Arg('--produced-by-rule'),
        Arg('--used-by-rule'),
        Arg('--produced-by-task'),
        Arg('--used-by-task'),
    ]
    sub_cmds = {
        'run': {
            'help': 'Run all pending tasks',
            'args': [
                Arg('remakefiles', nargs='*', default=['remakefile']),
                Arg('--rescan-only', action='store_true', help='only rescan input files'),
                Arg('--one', '-o', action='store_true', help='run one pending task'),
                Arg('--random',  action='store_true', help='run one (lucky dip!)'),
                *run_ctrl_group,
            ],
        },
        'run-tasks': {
            'help': 'Run specified tasks (uses same flags as ls-tasks)',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--tasks', '-t', nargs='*'),
                Arg('--handle-dependencies', '-H', action='store_true'),
                *run_ctrl_group,
                *task_filter_group,
            ]
        },
        'ls-rules': {
            'help': 'List rules',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--long', '-l', action='store_true'),
                Arg('--filter', '-F', default=None),
                Arg('--uses-file', '-U'),
                Arg('--produces-file', '-P'),
            ]
        },
        'ls-tasks': {
            'help': 'List tasks',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--long', '-l', action='store_true'),
                *task_filter_group,
            ]
        },
        'ls-files': {
            'help': 'Remove files',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--long', '-l', action='store_true'),
                *ls_files_group,
                Arg('--exists', action='store_true'),
            ]
        },
        'rm-files': {
            'help': 'Remove files',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--force', '-f', action='store_true'),
                *ls_files_group,
            ]
        },
        'info': {
            'help': 'Information about remakefile status',
            'args': [
                Arg('remakefiles', nargs='*', default=['remakefile']),
                Arg('--long', '-l', action='store_true'),
                Arg('--display', '-d', choices=['print_status', 'task_dag'],
                    default='print_status'),
            ]
        },
        'rule-info': {
            'help': 'Information about rule',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--long', '-l', action='store_true'),
                Arg('rules', nargs='*'),
            ]
        },
        'task-info': {
            'help': 'Information about task',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--long', '-l', action='store_true'),
                Arg('tasks', nargs='*'),
            ]
        },
        'file-info': {
            'help': 'Information about file',
            'args': [
                Arg('--long', '-l', action='store_true'),
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('filenames', nargs='*'),
            ]
        },
        'monitor': {
            'help': 'Monitor remake (polls remake metadata dir)',
            'args': [
                Arg('--timeout', '-t', help='timeout (s) to use for polling', default=10,
                    type=int),
                Arg('remakefile', nargs='?', default='remakefile'),
            ]
        },
        'version': {
            'help': 'Print remake version',
            'args': [
                Arg('--long', '-l', action='store_true', help='long version'),
            ]
        },
    }

    def __init__(self):
        self.args = None
        self.parser = self._build_parser()

    def _build_parser(self):
        parser = argparse.ArgumentParser(description='remake command line tool')
        parser._actions[0].help = 'Show this help message and exit'

        for argset in RemakeParser.args:
            add_argset(parser, argset)

        subparsers = parser.add_subparsers(dest='subcmd_name', required=True)
        for cmd_key, cmd_kwargs in RemakeParser.sub_cmds.items():
            args = cmd_kwargs['args']
            subparser = subparsers.add_parser(cmd_key, help=cmd_kwargs['help'])
            for argset in args:
                add_argset(subparser, argset)
        if argcomplete:
            argcomplete.autocomplete(parser)

        return parser

    def parse_args(self, argv: Optional[Sequence[Text]] = ...) -> argparse.Namespace:
        self.args = self.parser.parse_args(argv[1:])
        return self.args

    def dispatch(self):
        args = self.args
        # Dispatch command.
        # N.B. args should always be dereferenced at this point,
        # not passed into any subsequent functions.
        if args.subcmd_name == 'run':
            remake_run(args.remakefiles, args.rescan_only, args.force, args.one, args.random,
                       args.reasons, args.executor, args.display)
        elif args.subcmd_name == 'run-tasks':
            remake_run_tasks(args.remakefile, args.tasks, args.handle_dependencies, args.force,
                             args.reasons, args.executor, args.display,
                             args.filter, args.rule,
                             args.requires_rerun, args.uses_file, args.produces_file,
                             args.ancestor_of, args.descendant_of)
        elif args.subcmd_name == 'ls-rules':
            ls_rules(args.remakefile, args.long, args.filter, args.uses_file, args.produces_file)
        elif args.subcmd_name == 'ls-tasks':
            ls_tasks(args.remakefile, args.long,
                     args.filter, args.rule,
                     args.requires_rerun, args.uses_file,
                     args.produces_file, args.ancestor_of, args.descendant_of)
        elif args.subcmd_name in ['ls-files', 'rm-files']:
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
            if args.subcmd_name == 'ls-files':
                ls_files(args.remakefile, args.long, filetype, args.exists,
                         args.produced_by_rule, args.used_by_rule,
                         args.produced_by_task, args.used_by_task)
            else:
                rm_files(args.remakefile, args.force, filetype,
                         args.produced_by_rule, args.used_by_rule,
                         args.produced_by_task, args.used_by_task)
        elif args.subcmd_name == 'info':
            remakefile_info(args.remakefiles, args.long, args.display)
        elif args.subcmd_name == 'rule-info':
            rule_info(args.remakefile, args.long, args.rules)
        elif args.subcmd_name == 'task-info':
            task_info(args.remakefile, args.long, args.tasks)
        elif args.subcmd_name == 'file-info':
            file_info(args.remakefile, args.filenames)
        elif args.subcmd_name == 'monitor':
            monitor(args.remakefile, args.timeout)
        elif args.subcmd_name == 'version':
            print(get_version(form='long' if args.long else 'short'))
        else:
            assert False, f'Subcommand {args.subcmd_name} not recognized'


def remake_cmd(argv: Union[List[str], None] = None) -> None:
    if argv is None:
        argv = sys.argv
    parser = RemakeParser()
    args = parser.parse_args(argv)

    if args.debug_exception:
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info
    else:
        sys.excepthook = log_error

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

    if args.subcmd_name != 'monitor':
        setup_stdout_logging(loglevel, colour=colour)

    parser.dispatch()


def remake_run(remakefiles, rescan_only, force, one, random, print_reasons, executor, display):
    for remakefile in remakefiles:
        remake = load_remake(remakefile).finalize()
        remake.configure(print_reasons, executor, display)
        remake.short_status()
        if rescan_only:
            remake.task_ctrl.run_rescan_only()
        elif one:
            remake.run_one(force=force)
        elif random:
            remake.run_random(force=force)
        else:
            # TODO: not sure this works is force=True.
            remake.run_all(force=force)
        if display == 'task_dag':
            # Give user time to see final task_dag state.
            sleep(3)
        remake.short_status()


def remake_run_tasks(remakefile, task_path_hash_keys, handle_dependencies,
                     force, print_reasons, executor, display,
                     tfilter, rule,
                     requires_rerun, uses_file, produces_file,
                     ancestor_of, descendant_of):
    remake = load_remake(remakefile).finalize()
    remake.configure(print_reasons, executor, display)
    remake.short_status()
    if task_path_hash_keys and (tfilter or rule):
        raise RemakeError('Can only use one of --tasks and (--filter or --rule)')
    if task_path_hash_keys:
        tasks = remake.find_tasks(task_path_hash_keys)
    else:
        if tfilter:
            tfilter = dict([kv.split('=') for kv in tfilter.split(',')])
        tasks = remake.list_tasks(tfilter, rule, requires_rerun, uses_file,
                                  produces_file, ancestor_of, descendant_of)
    remake.run_requested(tasks, force=force, handle_dependencies=handle_dependencies)
    if display == 'task_dag':
        # Give user time to see final task_dag state.
        sleep(3)
    remake.short_status()


def ls_rules(remakefile, long, tfilter, uses_file, produces_file):
    # TODO: implement all args.
    remake = load_remake(remakefile)
    rules = remake.list_rules()
    for rule in rules:
        print(f'{rule.__name__}')


def ls_tasks(remakefile, long, tfilter, rule, requires_rerun, uses_file, produces_file,
             ancestor_of, descendant_of):
    remake = load_remake(remakefile).finalize()
    if tfilter:
        tfilter = dict([kv.split('=') for kv in tfilter.split(',')])
    tasks = remake.list_tasks(tfilter, rule, requires_rerun, uses_file,
                              produces_file, ancestor_of, descendant_of)
    tasks.status(long, long)


def ls_files(remakefile, long, filetype, exists,
             produced_by_rule, used_by_rule, produced_by_task, used_by_task):
    remake = load_remake(remakefile)
    filelist = remake.list_files(filetype, exists,
                                 produced_by_rule, used_by_rule, produced_by_task, used_by_task)
    if long:
        print(tabulate(filelist, headers=('path', 'filetype', 'exists')))
    else:
        for file, ftype, exists in filelist:
            print(file)


def rm_files(remakefile, force, filetype,
             produced_by_rule, used_by_rule, produced_by_task, used_by_task):
    remake = load_remake(remakefile)
    filelist = remake.list_files(filetype, True, produced_by_rule, used_by_rule,
                                 produced_by_task, used_by_task)
    if not filelist:
        logger.info('No files to delete')
        return

    if force:
        r = 'yes'
    else:
        r = input(bcolors.BOLD + bcolors.WARNING +
                  f'This will delete {len(filelist)} files, do you want to proceed? (yes/[no]): ' +
                  bcolors.ENDC)
    if r != 'yes':
        print('Not deleting files (yes not entered)')
        return
    for file, ftype, exists in filelist:
        if ftype == 'input-only':
            if force:
                r = 'yes'
            else:
                r = input(bcolors.BOLD + bcolors.FAIL +
                          f'Are you sure you want to delete input-only file: {file}? (yes/[no]): ' +
                          bcolors.ENDC)
            if r != 'yes':
                print('Not deleting files (yes not entered)')
                continue
        logger.info(f'Deleting file: {file}')
        file.unlink()


def remakefile_info(remakefiles, long, display):
    if display == 'print_status':
        if not long:
            rows = []
        for remakefile in remakefiles:
            remake = load_remake(remakefile).finalize()
            if not long:
                rows.append([remake.name,
                             len(remake.completed_tasks),
                             len(remake.task_ctrl.rescan_tasks),
                             len(remake.pending_tasks),
                             len(remake.remaining_tasks),
                             len(remake.task_ctrl.cannot_run_tasks),
                             len(remake.tasks),
                             ])
            else:
                print(f'{remake.name}')
                for i, task in enumerate(remake.sorted_tasks):
                    task_status = remake.task_status(task)
                    print(f'{i + 1}/{len(remake.tasks)}, {task_status}: {task.path_hash_key()}'
                          f' {task.short_str()}')

        if not long:
            # totals = list(np.array([r[1:] for r in rows]).sum(axis=0))
            # Same thing without numpy.
            totals = [sum(col) for col in list(zip(*rows))[1:]]
            rows.append(['Total'] + totals)
            print(tabulate(rows,
                           headers=('Name', 'completed', 'rescan', 'pending',
                                    'remaining', 'cannot run', 'total')))
    elif display == 'task_dag':
        for remakefile in remakefiles:
            remake = load_remake(remakefile).finalize()
            remake.display_task_dag()
    else:
        raise Exception(f'Unrecognized {display=}')


def rule_info(remakefile, long, rule_names):
    remake = load_remake(remakefile).finalize()
    rules = remake.list_rules()
    for rule_name in rule_names:
        found = False
        for rule in rules:
            if rule.__name__ == rule_name:
                print(rule)
                found = True
                break
        if not found:
            logger.error(f'No rule {rule_name} in {remake.name} found')


def task_info(remakefile, long, task_path_hash_keys):
    remake = load_remake(remakefile).finalize()
    info = remake.task_info(task_path_hash_keys)
    for task_path_hash_key, (task, task_md, status) in info.items():
        print(str(task))
        print(status)
        print(task_md.task_requires_rerun())
        if long:
            print('Uses files:')
            for key, path in task.inputs.items():
                print(f'  {key}: {path}')
            print('Produces files:')
            for key, path in task.outputs.items():
                print(f'  {key}: {path}')


def file_info(remakefile, filenames):
    remake = load_remake(remakefile).finalize()
    info = remake.file_info(filenames)
    for path, (path_md, produced_by_task, used_by_tasks) in info.items():
        if path.exists():
            print(f'exists: {path}')
        else:
            print(f'does not exist: {path}')
        if not path_md:
            print(f'Path not found in {remake.name}')
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


def monitor(remakefile, timeout):
    import logging
    from curses import wrapper

    remake_root = logging.getLogger('remake')
    handlers = [h for h in remake_root.handlers
                if isinstance(h, logging.StreamHandler)]
    for handler in handlers:
        remake_root.handlers.remove(handler)
    remake = load_remake(remakefile)
    remake.task_ctrl.build_task_DAG()
    wrapper(remake_curses_monitor, remake, timeout)
