import sys
import argparse
from hashlib import sha1
from pathlib import Path

from loguru import logger

from remake.loader import load_remake, load_archive

def log_error(ex_type, value, tb):
    import traceback
    traceback.print_exception(ex_type, value, tb)

    #if isinstance(value, RemakeError):
    #    logger.error(value)
    #else:
    #    import traceback

    #    traceback.print_exception(ex_type, value, tb)


def exception_info(ex_type, value, tb):
    import traceback

    traceback.print_exception(ex_type, value, tb)
    try:
        # Might not be installed.
        import ipdb as debug
    except ImportError:
        import pdb as debug
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
            Arg('--trace', '-T', help='Enable trace logging', action='store_true'),
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true', default=True),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
        Arg('--debug-exception', '-X', help='Launch pdb/ipdb on exception', action='store_true'),
        Arg('--return-remake', '-R', help='Return remake object', action='store_true'),
    ]
    sub_cmds = {
        'run': {
            'help': 'Run all pending tasks',
            'args': [
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--force', '-f', action='store_true'),
                Arg('remakefile', nargs='?', default='remakefile'),
            ],
        },
        'run-tasks': {
            'help': 'Run specified tasks (uses same flags as ls-tasks)',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--remakefile-sha1', default=None),
                Arg('--tasks', '-t', nargs='*'),
            ],
        },
        'info': {
            'help': 'Info on remake status of all tasks',
            'args': [
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('--show-failures', '-F', help='Show any failure messages', action='store_true'),
                Arg('--show-task-code-diff', '-D', help='Show any code diffs for class', action='store_true'),
                Arg('--show-reasons', '-R', help='Show reasons for rerun', action='store_true'),
                Arg('remakefile', nargs='?', default='remakefile'),
            ],
        },
        'ls-tasks': {
            'help': 'List specified tasks',
            'args': [
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('remakefile', nargs='?', default='remakefile'),
            ],
        },
        'run-tasks': {
            'help': 'Run specified tasks (uses same flags as ls-tasks)',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--remakefile-sha1', default=None),
                Arg('--tasks', '-t', nargs='*'),
            ],
        },
        'set-tasks-status': {
            'help': 'Set tasks status using given code (0=not run, 1=run, 2=failed)',
            'args': [
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--last-run-status-code', '-S', type=int),
            ],
        },
        'archive': {
            'help': 'archive the project according to info in archive.py',
            'args': []
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

        subparsers = parser.add_subparsers(dest='subcmd_name')
        for cmd_key, cmd_kwargs in RemakeParser.sub_cmds.items():
            args = cmd_kwargs['args']
            subparser = subparsers.add_parser(cmd_key, help=cmd_kwargs['help'])
            for argset in args:
                add_argset(subparser, argset)

        return parser

    def parse_args(self, argv):
        self.args = self.parser.parse_args(argv[1:])
        return self.args

    def dispatch(self):
        args = self.args
        self.rmk = None
        # Dispatch command.
        # N.B. args should always be dereferenced at this point,
        # not passed into any subsequent functions.
        logger.trace(args.subcmd_name)
        if args.subcmd_name == 'run':
            self.remake_run(args.remakefile, args.executor, args.query, args.force)
        elif args.subcmd_name == 'run-tasks':
            self.remake_run_tasks(args.remakefile, args.executor, args.remakefile_sha1, args.tasks)
        elif args.subcmd_name == 'ls-tasks':
            self.remake_ls_tasks(args.remakefile, args.query)
        elif args.subcmd_name == 'info':
            self.remake_info(args.remakefile, args.query, args.show_failures, args.show_task_code_diff, args.show_reasons)
        elif args.subcmd_name == 'set-tasks-status':
            self.remake_set_tasks_status(args.remakefile, args.query, args.last_run_status_code)
        elif args.subcmd_name == 'archive':
            self.remake_archive()
        return self.rmk

    def remake_run(self, remakefile, executor, query, force):
        rmk = load_remake(remakefile, run=True)
        rmk.run(executor=executor + 'Executor', query=query, force=force)
        self.rmk = rmk

    def remake_run_tasks(self, remakefile, executor, remakefile_sha1, task_keys):
        if remakefile_sha1:
            curr_remakefile_sha1 = sha1(Path(remakefile).read_bytes()).hexdigest()
            assert remakefile_sha1 == curr_remakefile_sha1

        rmk = load_remake(remakefile, finalize=False, run=True)
        rmk.run_tasks_from_keys(task_keys, executor=executor + 'Executor')
        self.rmk = rmk

    def remake_ls_tasks(self, remakefile, query):
        rmk = load_remake(remakefile, finalize=False)
        if query:
            tasks = rmk.topo_tasks.where(query)
        else:
            tasks = rmk.topo_tasks
        for task in tasks:
            print(task)
        self.rmk = rmk

    def remake_set_tasks_status(self, remakefile, query, last_run_status_code):
        rmk = load_remake(remakefile)
        if query:
            tasks = rmk.topo_tasks.where(query[0])
        else:
            tasks = rmk.topo_tasks

        r = input(f'Set status for {len(tasks)} task(s)? y/[n] ')
        if r == 'y':
            for task in tasks:
                task.last_run_status = last_run_status_code
                rmk.update_task(task)
        self.rmk = rmk

    def remake_info(self, remakefile, query, show_failures, show_task_code_diff, show_reasons):
        rmk = load_remake(remakefile)
        # print(rmk.name)
        status_map = {
            0: 'R',
            1: 'C',
            2: 'RF',
        }
        for task in rmk.topo_tasks:
            status = status_map[task.last_run_status]
            if task.requires_rerun and 'R' not in status:
                status = 'R'
            if task.inputs_missing:
                status = 'X' + status
            task.status = status

        if query:
            print('Filter on: ', query[0])
            filtered_tasks = rmk.topo_tasks.where(query[0])
        else:
            filtered_tasks = rmk.topo_tasks

        for task in filtered_tasks:
            print(f'{task.status:<2s} {task}')
            if 'F' in task.status and show_failures:
                print('==>  FAILURE TRACEBACK  <==')
                print(task.last_run_exception)
                print('==>END FAILURE TRACEBACK<==')
            if ('R' in task.status or 'X' in task.status) and show_reasons:
                for reason in task.rerun_reasons:
                    print(f'   - {reason}')
            if show_task_code_diff and 'task_run_source_changed' in task.rerun_reasons:
                print('==>  DIFF  <==')
                print('\n'.join(task.diff()))
                print('==>END DIFF<==')
        self.rmk = rmk

    def remake_archive(self):
        archive = load_archive('archive.py')
        for f in dir(archive):
            if not f.startswith('__'):
                print(f'{f} = {getattr(archive, f)}')
        rmk = load_remake(archive.remakefile)
        for rule in rmk.rules:
            if hasattr(rule, 'archive'):
                print([p for t in rule.tasks for p in t.inputs.values()])



def remake_cmd(argv=None):
    if argv is None:
        argv = sys.argv
    parser = RemakeParser()
    args = parser.parse_args(argv)
    if not args.subcmd_name:
        parser.parser.print_help()
        return 1

    logger.remove()
    if args.trace:
        loglevel = 'TRACE'
        logger.add(sys.stdout, level=loglevel)
    elif args.debug:
        loglevel = 'DEBUG'
        logger.add(sys.stdout, level=loglevel)
    elif args.info:
        loglevel = 'INFO'
        logger.add(sys.stdout, format='<bold>{message}</bold>', level=loglevel)
    elif args.warning:
        loglevel = 'WARNING'
        logger.add(sys.stdout, format='<bold>{message}</bold>', level=loglevel)

    logger.debug('Called with args:')
    logger.debug(argv)

    if args.debug_exception:
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info
    else:
        sys.excepthook = log_error

    if args.return_remake:
        return parser.dispatch()
    else:
        parser.dispatch()

    if args.return_remake:
        return parser.rmk


if __name__ == '__main__':
    rmk = remake_cmd()
