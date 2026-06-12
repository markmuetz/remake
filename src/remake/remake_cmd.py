"""remake command line tool.

Minimal surface: run, run-task, info, version. Declarative arg definitions
and method-name dispatch carried over from remake2.
"""
import argparse
import sys
from collections import Counter
from pathlib import Path

from loguru import logger

from .core import RemakeError
from .loader import load_remake
from .metadata import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from .util import Arg, MutuallyExclusiveGroup, add_argset
from .version import __version__

STATUS_NAMES = {
    None: 'pending',
    TASK_STATUS_SUCCESS: 'success',
    TASK_STATUS_FAILED: 'failed',
}

def _make_executor(name, rmk):
    """Resolve an executor: a builtin name, or a user class given as a
    dotted path ('mymodule:MyExecutor' or 'mymodule.MyExecutor')."""
    import importlib

    from .executors import Executor, SingleprocExecutor, SlurmExecutor

    builtin = {'singleproc': SingleprocExecutor, 'slurm': SlurmExecutor}
    if name in builtin:
        return builtin[name](rmk)

    if ':' in name:
        module_name, _, cls_name = name.partition(':')
    elif '.' in name:
        module_name, _, cls_name = name.rpartition('.')
    else:
        raise RemakeError(
            f'Unknown executor {name!r}: use one of {sorted(builtin)} or a '
            f'dotted path like mymodule:MyExecutor'
        )
    cls = getattr(importlib.import_module(module_name), cls_name)
    if not (isinstance(cls, type) and issubclass(cls, Executor)):
        raise RemakeError(f'{name!r} is not an Executor subclass')
    return cls(rmk)


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
    """Command line args and dispatch."""

    args = [
        MutuallyExclusiveGroup(
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true', default=True),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
        Arg('--debug-exception', '-X', help='Launch pdb/ipdb on exception', action='store_true'),
    ]
    sub_cmds = {
        'run': {
            'help': 'Run all pending tasks',
            'args': [
                Arg('remakefile'),
                Arg('--executor', '-E', default='singleproc',
                    help='Builtin executor name or dotted path to an '
                         'Executor subclass (mymodule:MyExecutor)'),
                Arg('--query', '-Q', help='Filter tasks based on a kwargs query'),
                Arg('--force', '-f', help='Force rerun of matched tasks', action='store_true'),
                Arg('--dry-run', '-n', help='Show what would run, run nothing',
                    action='store_true'),
                Arg('--check-outputs', help='Verify outputs of completed tasks (always mode)',
                    action='store_true'),
            ],
        },
        'run-task': {
            'help': 'Run a single task by its key',
            'args': [
                Arg('remakefile'),
                Arg('task_key'),
            ],
        },
        'run-array-task': {
            'help': 'Run one task from a generated job spec (used by SLURM jobs)',
            'args': [
                Arg('remakefile'),
                Arg('rule'),
                Arg('index', type=int),
            ],
        },
        'resubmit': {
            'help': 'Re-execute .remake/submit.sh without replanning',
            'args': [
                Arg('remakefile'),
            ],
        },
        'info': {
            'help': 'Per-rule summary of task statuses',
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', help='Filter tasks based on a kwargs query'),
                Arg('--tasks', '-t', help='List individual tasks with status',
                    action='store_true'),
                Arg('--show-failures', '-F', help='Show stored tracebacks of failed tasks',
                    action='store_true'),
            ],
        },
        'version': {
            'help': 'Print remake version',
            'args': [],
        },
    }

    def __init__(self):
        self.args = None
        self.parser = self._build_parser()

    def _build_parser(self):
        parser = argparse.ArgumentParser(description='remake command line tool')
        for argset in RemakeParser.args:
            add_argset(parser, argset)

        subparsers = parser.add_subparsers(dest='subcmd_name')
        for cmd_key, cmd_kwargs in RemakeParser.sub_cmds.items():
            subparser = subparsers.add_parser(cmd_key, help=cmd_kwargs['help'])
            for argset in cmd_kwargs['args']:
                add_argset(subparser, argset)

        return parser

    def parse_args(self, argv):
        self.args = self.parser.parse_args(argv[1:])
        return self.args

    def dispatch(self):
        args = self.args
        method_name = 'remake_' + args.subcmd_name.replace('-', '_')
        getattr(self, method_name)(args)

    def _load(self, args):
        rmk = load_remake(args.remakefile)
        if getattr(args, 'check_outputs', False):
            rmk.check_outputs = 'always'
        return rmk

    def remake_run(self, args):
        rmk = self._load(args)
        executor = _make_executor(args.executor, rmk)
        if args.dry_run:
            if executor.supports_dry_run:
                executor.dry_run = True
            else:
                runnable, deferred = rmk.plan(query=args.query, force=args.force)
                for task in runnable:
                    print(task)
                print(f'{len(runnable)} task(s) would run')
                for rule in deferred:
                    print(f'{rule.name}: deferred (matrix not ready)')
                return
        rmk.run(executor=executor, query=args.query, force=args.force)

    def remake_run_task(self, args):
        rmk = self._load(args)
        task = rmk.task_from_key(args.task_key)
        logger.info(f'Running {task}')
        rmk.run_task(task)

    def remake_run_array_task(self, args):
        import json

        rmk = self._load(args)
        specs = json.loads(Path(f'.remake/jobs/{args.rule}.json').read_text())
        spec = specs[args.index]
        task = rmk.task_from_spec(spec['rule'], spec['kwargs'])
        logger.info(f'Running {task}')
        rmk.run_task(task)

    def remake_resubmit(self, args):
        import subprocess as sp

        submit = Path('.remake/submit.sh')
        if not submit.exists():
            raise RemakeError(
                f'No {submit} — generate it with: remake run {args.remakefile} --executor slurm'
            )
        result = sp.run(['bash', str(submit)], capture_output=True, text=True)
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.returncode != 0:
            print(result.stderr.strip(), file=sys.stderr)
            raise RemakeError(f'{submit} failed (exit {result.returncode})')

    def remake_info(self, args):
        from .core import expand_rule
        from .core.planner import make_predicate

        rmk = self._load(args)
        runnable, deferred = rmk.plan(query=args.query)
        remaining = Counter(task.rule.name for task in runnable)
        deferred_names = {rule.name for rule in deferred}
        predicate = make_predicate(args.query) if args.query else None

        header = ('rule', 'tasks', 'success', 'failed', 'pending', 'to run')
        rows = []
        task_lines = []
        failures = []
        for rule in rmk.rules:
            if rule.name in deferred_names:
                rows.append((rule.name, '?', '?', '?', '?', 'deferred'))
                continue
            tasks = expand_rule(rule, predicate)
            records = rmk.metadata.get_tasks_status(tasks)
            counts = Counter(
                STATUS_NAMES.get(records[t.key].status, 'pending')
                if t.key in records
                else 'pending'
                for t in tasks
            )
            rows.append(
                (
                    rule.name,
                    len(tasks),
                    counts['success'],
                    counts['failed'],
                    counts['pending'],
                    remaining.get(rule.name, 0),
                )
            )
            if args.tasks:
                for task in tasks:
                    record = records.get(task.key)
                    status = STATUS_NAMES.get(record.status, 'pending') if record else 'pending'
                    task_lines.append(f'{status:<8} {task}')
            if args.show_failures:
                failures.extend(
                    (task, records[task.key])
                    for task in tasks
                    if task.key in records
                    and records[task.key].status == TASK_STATUS_FAILED
                )

        widths = [max(len(str(r[i])) for r in rows + [header]) for i in range(len(header))]
        for row in [header] + rows:
            print('  '.join(f'{str(v):<{w}}' for v, w in zip(row, widths)))
        for line in task_lines:
            print(line)
        for task, record in failures:
            print(f'\n=== {task} (failed at {record.timestamp}) ===')
            print(record.exception.rstrip() or '(no stored exception)')

    def remake_version(self, args):
        print(__version__)


def remake_cmd(argv=None):
    """Main entry point."""
    if argv is None:
        argv = sys.argv
    parser = RemakeParser()
    args = parser.parse_args(argv)
    if not args.subcmd_name:
        parser.parser.print_help()
        return 1

    logger.remove()
    if args.debug:
        logger.add(sys.stdout, colorize=True, level='DEBUG')
    elif args.warning:
        logger.add(
            sys.stdout, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='WARNING'
        )
    else:
        logger.add(
            sys.stdout, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='INFO'
        )

    if hasattr(args, 'remakefile'):
        # Always-on DEBUG file log next to the metadata DB — per-job logs are
        # how cluster failures get debugged.
        logfile = Path('.remake/remake.log')
        logfile.parent.mkdir(parents=True, exist_ok=True)
        logger.add(logfile, level='DEBUG', rotation='5 MB', retention=3)
        logger.debug(f'argv: {argv}')

    if args.debug_exception:
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info

    parser.dispatch()


if __name__ == '__main__':
    remake_cmd()
