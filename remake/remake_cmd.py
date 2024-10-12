import sys
import argparse
from hashlib import sha1
from pathlib import Path

from loguru import logger

# Note, relative imports do not always work for this entry point.
from remake.loader import load_remake, load_archive, load_module
from remake.util import Arg, MutuallyExclusiveGroup, add_argset, sysrun
from remake.core import restore


def log_error(ex_type, value, tb):
    import traceback

    traceback.print_exception(ex_type, value, tb)

    # if isinstance(value, RemakeError):
    #    logger.error(value)
    # else:
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


class RemakeParser:
    """Command line args and dispatch"""

    args = [
        MutuallyExclusiveGroup(
            Arg('--trace', '-T', help='Enable trace logging', action='store_true'),
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true', default=True),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
        Arg('--log-filter', '-L', help='Filter log based on value', default=''),
        Arg('--debug-exception', '-X', help='Launch pdb/ipdb on exception', action='store_true'),
        Arg('--return-remake', '-R', help='Return remake object', action='store_true'),
    ]
    sub_cmds = {
        'run': {
            'help': 'Run all pending tasks',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--force', '-f', action='store_true'),
                Arg('--show-reasons', '-R', help='Show reasons for rerun', action='store_true'),
                Arg(
                    '--show-task-code-diff',
                    '-D',
                    help='Show any code diffs for class',
                    action='store_true',
                ),
                Arg(
                    '--stdout-to-log',
                    '-S',
                    help='Capture and log stdout instead of displaying',
                    action='store_true',
                ),
                Arg('--number', '-N', help='Just run first N tasks', default='all'),
            ],
        },
        'run-tasks': {
            'help': 'Run specified tasks (uses same flags as ls-tasks)',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--remakefile-sha1', default=None),
                Arg('--tasks', '-t', nargs='*'),
            ],
        },
        'info': {
            'help': 'Info on remake status of all tasks',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('--show-reasons', '-R', help='Show reasons for rerun', action='store_true'),
                Arg('--show-failures', '-F', help='Show any failure messages', action='store_true'),
                Arg(
                    '--show-task-code-diff',
                    '-D',
                    help='Show any code diffs for class',
                    action='store_true',
                ),
                Arg('--short', '-S', help='Short output', action='store_true'),
                Arg('--rule', help='Show summary for each rule', action='store_true'),
                Arg('--status', help='Filter on status', nargs='?'),
            ],
        },
        'touch': {
            'help': 'Use touch to create/update timestamps of files',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                MutuallyExclusiveGroup(
                    Arg(
                        '--inputs',
                        '-I',
                        help='touch inputs-only files',
                        action='store_true',
                        default=True,
                    ),
                    Arg(
                        '--all',
                        '-A',
                        help='touch all files in topological order',
                        action='store_true',
                    ),
                ),
            ],
        },
        'ls-tasks': {
            'help': 'List specified tasks',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
            ],
        },
        'set-tasks-status': {
            'help': 'Set tasks status using given code (0=not run, 1=run, 2=failed)',
            'args': [
                Arg('remakefile', nargs='?', default=''),
                Arg('--query', '-Q', help='Filter tasks based on query', nargs=1),
                Arg('--last-run-status-code', '-S', type=int),
            ],
        },
        'archive': {
            'help': 'archive the project according to info in archive.py',
            'args': [
                Arg('archive', nargs='?', default='archive.py'),
                Arg('--executor', '-E', default='Singleproc'),
                Arg('--dry-run', '-n', action='store_true'),
            ],
        },
        'restore': {
            'help': 'restore the project',
            'args': [
                Arg('archive', nargs='?', default=''),
                Arg('--data-dir', default=None),
            ],
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
        if hasattr(args, 'remakefile') and not args.remakefile:
            if Path('.remake/config.py').exists():
                c = load_module('.remake/config.py')
                if hasattr(c, 'config'):
                    args.remakefile = c.config.get('default_remakefile', '')
                    logger.debug(
                        f'loaded remakefile name from .remake/config.py: {args.remakefile}'
                    )
            if not args.remakefile:
                raise Exception('remakefile must be set')

        self.rmk = None
        # Dispatch command.
        logger.trace(args.subcmd_name)
        method_name = 'remake_{subcmd_name}'.format(subcmd_name=args.subcmd_name.replace('-', '_'))
        dispatch_method = getattr(self, method_name)
        dispatch_method(args)

        return self.rmk

    def remake_run(self, args):
        rmk = load_remake(args.remakefile, run=True)
        if args.query:
            query = args.query[0]
        else:
            query = None
        rmk.run(
            executor=args.executor + 'Executor',
            query=query,
            force=args.force,
            show_reasons=args.show_reasons,
            show_task_code_diff=args.show_task_code_diff,
            stdout_to_log=args.stdout_to_log,
            number=args.number,
        )
        self.rmk = rmk

    def remake_run_tasks(self, args):
        if args.remakefile_sha1:
            curr_remakefile_sha1 = sha1(Path(args.remakefile).read_bytes()).hexdigest()
            assert args.remakefile_sha1 == curr_remakefile_sha1

        task_keys = args.tasks
        rmk = load_remake(args.remakefile, finalize=False, run=True)
        rmk.run_tasks_from_keys(task_keys, executor=args.executor + 'Executor')
        self.rmk = rmk

    def remake_ls_tasks(self, args):
        rmk = load_remake(args.remakefile, finalize=False)
        if args.query:
            tasks = rmk.topo_tasks.where(args.query[0])
        else:
            tasks = rmk.topo_tasks
        for task in tasks:
            print(task)
        self.rmk = rmk

    def remake_set_tasks_status(self, args):
        rmk = load_remake(args.remakefile)
        if args.query:
            tasks = rmk.topo_tasks.where(args.query[0])
        else:
            tasks = rmk.topo_tasks

        r = input(f'Set status for {len(tasks)} task(s)? y/[n] ')
        if r == 'y':
            for task in tasks:
                task.last_run_status = args.last_run_status_code
                rmk.update_task(task)
        self.rmk = rmk

    def remake_info(self, args):
        rmk = load_remake(args.remakefile)
        query = args.query[0] if args.query else None
        rmk.info(
            query,
            args.show_failures,
            args.show_reasons,
            args.show_task_code_diff,
            args.short,
            args.rule,
            args.status,
        )
        self.rmk = rmk

    def remake_touch(self, args):
        rmk = load_remake(args.remakefile)
        rmk.touch(args.inputs, args.all)
        self.rmk = rmk

    def remake_archive(self, args):
        archive = load_archive(args.archive)
        # for f in dir(archive):
        #     if not f.startswith('__'):
        #         print(f'{f} = {getattr(archive, f)}')
        rmk = load_remake(archive.remakefile)
        archive.add_remake(rmk)
        archive.archive(args.archive, args.dry_run, executor=args.executor + 'Executor')

    def remake_restore(self, args):
        restore(args.archive, args.data_dir)


def remake_cmd(argv=None):
    """Main entry point"""
    if argv is None:
        argv = sys.argv
    parser = RemakeParser()
    args = parser.parse_args(argv)
    if not args.subcmd_name:
        parser.parser.print_help()
        return 1

    logger.remove()
    if args.trace:
        logger.add(sys.stdout, colorize=True, filter=args.log_filter, level='TRACE')
    elif args.debug:
        logger.add(sys.stdout, colorize=True, filter=args.log_filter, level='DEBUG')
    elif args.info:
        logger.add(
            sys.stdout, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='INFO'
        )
    elif args.warning:
        logger.add(
            sys.stdout, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='WARNING'
        )

    if args.subcmd_name not in {'restore'}:
        if hasattr(args, 'remakefile'):
            file_log = f'.remake/log/{args.remakefile}/remake.log'
        else:
            file_log = f'.remake/log/remake.log'
        logger.add(file_log, rotation='00:00', level='TRACE' if args.trace else 'DEBUG')

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


if __name__ == '__main__':
    rmk = remake_cmd()
