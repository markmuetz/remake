import sys
import argparse
from hashlib import sha1
from pathlib import Path

from loguru import logger

from .loader import load_remake

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
                Arg('remakefile', nargs='?', default='remakefile'),
                Arg('--executor', '-E', default='Singleproc'),
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
        'ls-tasks': {
            'help': 'List specified tasks',
            'args': [
                Arg('remakefile', nargs='?', default='remakefile'),
            ],
        },
    }

    def __init__(self):
        self.args = None
        self.parser = self._build_parser()

    def _build_parser(self):
        parser = argparse.ArgumentParser(description='remake2 command line tool')
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
        if args.subcmd_name == 'run':
            self.remake_run(args.remakefile, args.executor)
        elif args.subcmd_name == 'run-tasks':
            self.remake_run_tasks(args.remakefile, args.executor, args.remakefile_sha1, args.tasks)
        elif args.subcmd_name == 'ls-tasks':
            self.remake_ls_tasks(args.remakefile)
        return self.rmk

    def remake_run(self, remakefile, executor):
        rmk = load_remake(remakefile)
        rmk.run(executor=executor + 'Executor')
        self.rmk = rmk

    def remake_run_tasks(self, remakefile, executor, remakefile_sha1, task_keys):
        if remakefile_sha1:
            curr_remakefile_sha1 = sha1(Path(remakefile).read_bytes()).hexdigest()
            assert remakefile_sha1 == curr_remakefile_sha1

        rmk = load_remake(remakefile, finalize=False)
        rmk.run_tasks_from_keys(task_keys, executor=executor + 'Executor')
        self.rmk = rmk

    def remake_ls_tasks(self, remakefile):
        rmk = load_remake(remakefile)
        for task in rmk.tasks:
            print(task)
        self.rmk = rmk


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
    remake_cmd()