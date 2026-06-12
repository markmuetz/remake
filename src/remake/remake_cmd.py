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
from .util import Arg, MutuallyExclusiveGroup, add_argset, task_log_path as _task_log_path
from .version import __version__

STATUS_NAMES = {
    None: 'pending',
    TASK_STATUS_SUCCESS: 'success',
    TASK_STATUS_FAILED: 'failed',
}

def _add_task_log_sink(task):
    """One process, one file — safe under concurrent SLURM array elements,
    unlike the shared log."""
    logfile = _task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    logger.add(logfile, level='DEBUG', mode='w')


def _select_task(rmk, args):
    """Resolve the task a command addresses: a key prefix, or a -Q query
    matching exactly one task."""
    if args.task_key and args.query:
        raise RemakeError('Give a task key or a -Q query, not both')
    if args.task_key:
        return rmk.task_from_key(args.task_key)
    if args.query:
        tasks = rmk.tasks(query=args.query)
        if not tasks:
            raise RemakeError(f'No task matches {args.query!r}')
        if len(tasks) > 1:
            raise RemakeError(
                f'{len(tasks)} tasks match {args.query!r}; narrow the query '
                f"(add `rule == '<name>'` if the matrix is shared between rules)"
            )
        return tasks[0]
    raise RemakeError('Give a task key prefix or a -Q query')


def _slurm_submission(rule_name, task_key=None):
    """(jobids, array_index) from the last submission's sidecar/job spec,
    (None, None) if never submitted."""
    import json

    sidecar = Path(f'.remake/jobs/{rule_name}.jobids.json')
    if not sidecar.exists():
        return None, None
    recorded = json.loads(sidecar.read_text())
    jobids = recorded.get('slurm_job_ids', [])
    if 'slurm_array_job_id' in recorded:
        jobids = [recorded['slurm_array_job_id']]
    index = None
    specs_path = Path(f'.remake/jobs/{rule_name}.json')
    if task_key is not None and specs_path.exists():
        specs = json.loads(specs_path.read_text())
        index = next((i for i, s in enumerate(specs) if s['task_key'] == task_key), None)
    return jobids, index


def _make_executor(name, rmk, nproc=None):
    """Resolve an executor: a builtin name, or a user class given as a
    dotted path ('mymodule:MyExecutor' or 'mymodule.MyExecutor')."""
    import importlib

    from .executors import Executor, MultiprocExecutor, SingleprocExecutor, SlurmExecutor

    if name == 'multiproc':
        return MultiprocExecutor(rmk, nproc=nproc)
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
    # Not pm(): sys.last_traceback is only set by the interactive
    # interpreter, not inside an excepthook.
    debug.post_mortem(tb)


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
                    help='singleproc, multiproc, slurm, or dotted path to an '
                         'Executor subclass (mymodule:MyExecutor)'),
                Arg('--nproc', '-j', type=int,
                    help='Worker processes for the multiproc executor '
                         '(default: all cores)'),
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
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'ls-tasks': {
            'help': 'List tasks (key prefix + name), materialising the matrices',
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', help='Filter tasks based on a kwargs query'),
                Arg('--json', help='Machine-readable output (full keys)', action='store_true'),
            ],
        },
        'lint': {
            'help': 'Check input/output wiring between rules (near-misses, '
                    'missing depends_on)',
            'args': [
                Arg('remakefile'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'task-info': {
            'help': 'Detail view of one task: status, paths, log, SLURM job',
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q', help='Select the task by kwargs query instead of key'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'task-log': {
            'help': "Print a task's per-task log",
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q', help='Select the task by kwargs query instead of key'),
                Arg('--path', help='Print the log path only', action='store_true'),
            ],
        },
        'why': {
            'help': 'Explain why a task would (or would not) rerun',
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q', help='Select the task by kwargs query instead of key'),
            ],
        },
        'slurm-status': {
            'help': 'Live SLURM queue state of the last submission, per rule',
            'args': [
                Arg('remakefile'),
                Arg('--json', help='Machine-readable output', action='store_true'),
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
        return getattr(self, method_name)(args)

    def _load(self, args):
        rmk = load_remake(args.remakefile)
        if getattr(args, 'check_outputs', False):
            rmk.check_outputs = 'always'
        return rmk

    def remake_run(self, args):
        rmk = self._load(args)
        executor = _make_executor(args.executor, rmk, nproc=args.nproc)
        # -X: first task failure propagates (into the pdb/ipdb excepthook)
        # instead of being recorded-and-continued.
        executor.raise_on_failure = args.debug_exception
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
        nfailed = rmk.run(executor=executor, query=args.query, force=args.force)
        return 1 if nfailed else 0

    def remake_run_task(self, args):
        rmk = self._load(args)
        task = rmk.task_from_key(args.task_key)
        _add_task_log_sink(task)
        logger.info(f'Running {task}')
        rmk.run_task(task)

    def remake_run_array_task(self, args):
        import json

        from .metadata.sidecar import SidecarWriter

        # Hundreds of concurrent array elements must not touch the shared
        # SQLite DB (livelock on shared filesystems): load without
        # finalizing (no ensure_rules, no DB connection) and record the
        # result as a sidecar file, ingested by the next plan/info.
        rmk = load_remake(args.remakefile, finalize=False)
        rmk.metadata = SidecarWriter()
        specs = json.loads(Path(f'.remake/jobs/{args.rule}.json').read_text())
        spec = specs[args.index]
        task = rmk.task_from_spec(spec['rule'], spec['kwargs'])
        _add_task_log_sink(task)
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
        import json

        from .core import expand_rule
        from .core.planner import make_predicate

        rmk = self._load(args)
        runnable, deferred = rmk.plan(query=args.query)
        remaining = Counter(task.rule.name for task in runnable)
        deferred_names = {rule.name for rule in deferred}
        predicate = make_predicate(args.query) if args.query else None

        rule_rows = []
        task_rows = []
        failures = []
        for rule in rmk.rules:
            if rule.name in deferred_names:
                rule_rows.append({'rule': rule.name, 'deferred': True})
                continue
            tasks = expand_rule(rule, predicate)
            records = rmk.metadata.get_tasks_status(tasks)
            statuses = {
                t.key: STATUS_NAMES.get(records[t.key].status, 'pending')
                if t.key in records
                else 'pending'
                for t in tasks
            }
            counts = Counter(statuses.values())
            rule_rows.append(
                {
                    'rule': rule.name,
                    'deferred': False,
                    'tasks': len(tasks),
                    'success': counts['success'],
                    'failed': counts['failed'],
                    'pending': counts['pending'],
                    'to_run': remaining.get(rule.name, 0),
                }
            )
            if args.tasks:
                task_rows.extend(
                    {'task': str(t), 'key': t.key, 'status': statuses[t.key]} for t in tasks
                )
            if args.show_failures:
                failures.extend(
                    {
                        'task': str(t),
                        'key': t.key,
                        'timestamp': records[t.key].timestamp,
                        'exception': records[t.key].exception,
                        'log': str(_task_log_path(t)),
                    }
                    for t in tasks
                    if t.key in records and records[t.key].status == TASK_STATUS_FAILED
                )

        if args.json:
            data = {'rules': rule_rows}
            if args.tasks:
                data['tasks'] = task_rows
            if args.show_failures:
                data['failures'] = failures
            print(json.dumps(data, indent=1))
            return

        header = ('rule', 'tasks', 'success', 'failed', 'pending', 'to run')
        rows = [
            (r['rule'], '?', '?', '?', '?', 'deferred')
            if r['deferred']
            else (r['rule'], r['tasks'], r['success'], r['failed'], r['pending'], r['to_run'])
            for r in rule_rows
        ]
        widths = [max(len(str(r[i])) for r in rows + [header]) for i in range(len(header))]
        for row in [header] + rows:
            print('  '.join(f'{str(v):<{w}}' for v, w in zip(row, widths)))
        for task_row in task_rows:
            print(f'{task_row["status"]:<8} {task_row["task"]}')
        for failure in failures:
            print(f'\n=== {failure["task"]} (failed at {failure["timestamp"]}) ===')
            print(f'log: {failure["log"]}')
            print(failure['exception'].rstrip() or '(no stored exception)')

    def remake_ls_tasks(self, args):
        import json

        from .core.dag import iter_expand_rule
        from .core.exceptions import MatrixNotReady
        from .core.planner import make_predicate

        rmk = self._load(args)
        rmk.finalize()
        predicate = make_predicate(args.query) if args.query else None
        rows = []
        for rule in rmk.rules:
            try:
                # Stream in text mode: constant memory however big the matrix.
                for task in iter_expand_rule(rule, predicate):
                    if args.json:
                        rows.append(
                            {'key': task.key, 'rule': rule.name, 'kwargs': task.kwargs}
                        )
                    else:
                        print(task)
            except MatrixNotReady:
                logger.warning(f'{rule.name}: deferred (matrix not ready), tasks unknown')
        if args.json:
            print(json.dumps(rows, indent=1))

    def remake_lint(self, args):
        """Wiring check: every input of a dependent rule should be produced
        by one of its depends_on rules. Flags near-miss paths (typos in
        format strings, off-by-one kwargs) and produced-but-undeclared
        dependencies. Materialises all tasks."""
        import difflib
        import json

        from .core.dag import iter_expand_rule
        from .core.exceptions import MatrixNotReady

        rmk = self._load(args)
        rmk.finalize()

        producers = {}  # output path -> set of rule names
        rule_outputs = {}  # rule name -> [output paths]
        deferred = set()
        for rule in rmk.rules:
            paths = []
            try:
                for task in iter_expand_rule(rule):
                    paths.extend(str(token) for token in task.outputs.values())
            except MatrixNotReady:
                deferred.add(rule.name)
                logger.warning(f'{rule.name}: matrix not ready, outputs unknown — skipped')
                continue
            rule_outputs[rule.name] = paths
            for path in paths:
                producers.setdefault(path, set()).add(rule.name)

        findings = {}  # (kind, rule, other) -> {'count': n, 'example': ...}

        def record(kind, rule_name, other, example):
            entry = findings.setdefault(
                (kind, rule_name, other), {'count': 0, 'example': example}
            )
            entry['count'] += 1

        for rule in rmk.rules:
            if rule.inputs is None or rule.name in deferred:
                continue
            dep_names = {dep.name for dep in rule.depends_on}
            if dep_names & deferred:
                logger.warning(f'{rule.name}: upstream matrix not ready — skipped')
                continue
            candidates = [p for name in dep_names for p in rule_outputs.get(name, [])]
            for task in iter_expand_rule(rule):
                for path in map(str, task.inputs.values()):
                    made_by = producers.get(path)
                    if made_by:
                        if not made_by & (dep_names | {rule.name}):
                            record('missing_dependency', rule.name, min(made_by), path)
                        continue
                    if not dep_names:
                        record('external', rule.name, None, path)
                        continue
                    close = difflib.get_close_matches(path, candidates, n=1, cutoff=0.9)
                    if close:
                        producer = min(producers[close[0]])
                        record(
                            'near_miss', rule.name, producer, {'input': path, 'closest': close[0]}
                        )
                    else:
                        record('external', rule.name, None, path)

        rows = [
            {'kind': kind, 'rule': rule_name, 'other_rule': other, **entry}
            for (kind, rule_name, other), entry in sorted(
                findings.items(), key=lambda kv: (kv[0][0] != 'near_miss', kv[0])
            )
        ]
        problems = [r for r in rows if r['kind'] in ('near_miss', 'missing_dependency')]
        if args.json:
            print(json.dumps(rows, indent=1))
            return 1 if problems else 0

        for row in rows:
            if row['kind'] == 'near_miss':
                ex = row['example']
                print(
                    f"NEAR MISS           {row['rule']}: input {ex['input']!r} is produced "
                    f"by nothing, but {row['other_rule']} produces {ex['closest']!r} "
                    f"({row['count']} task(s))"
                )
            elif row['kind'] == 'missing_dependency':
                print(
                    f"MISSING DEPENDENCY  {row['rule']}: input {row['example']!r} is "
                    f"produced by {row['other_rule']}, which {row['rule']} does not "
                    f"depend_on ({row['count']} task(s))"
                )
            else:
                print(
                    f"external            {row['rule']}: {row['count']} input(s) not "
                    f"produced by any rule (e.g. {row['example']!r})"
                )
        if not rows:
            print('all inputs wired to declared dependencies')
        return 1 if problems else 0

    def remake_task_info(self, args):
        import json

        rmk = self._load(args)
        task = _select_task(rmk, args)
        record = rmk.metadata.get_tasks_status([task]).get(task.key)
        log_path = _task_log_path(task)
        jobids, array_index = _slurm_submission(task.rule.name, task.key)
        inputs = (
            {k: {'path': str(v), 'exists': Path(v).exists()} for k, v in task.inputs.items()}
            if task.rule.inputs is not None
            else {}
        )
        outputs = (
            {k: {'path': str(v), 'complete': v.is_complete()} for k, v in task.outputs.items()}
            if task.rule.outputs is not None
            else {}
        )
        data = {
            'task': str(task),
            'rule': task.rule.name,
            'kwargs': task.kwargs,
            'key': task.key,
            'status': STATUS_NAMES.get(record.status, 'pending') if record else 'pending',
            'timestamp': record.timestamp if record else None,
            'exception': record.exception if record else '',
            'inputs': inputs,
            'outputs': outputs,
            'log': {'path': str(log_path), 'exists': log_path.exists()},
            'slurm': {'jobids': jobids, 'array_index': array_index},
        }
        if args.json:
            print(json.dumps(data, indent=1))
            return

        print(f'{task}  {task.key}')
        when = f' at {data["timestamp"]}' if data['timestamp'] else ''
        print(f'status:   {data["status"]}{when}')
        for name, path_info in inputs.items():
            mark = 'exists' if path_info['exists'] else 'MISSING'
            print(f'input:    {path_info["path"]}  [{mark}]  ({name})')
        for name, path_info in outputs.items():
            mark = 'complete' if path_info['complete'] else 'missing'
            print(f'output:   {path_info["path"]}  [{mark}]  ({name})')
        mark = '' if log_path.exists() else '  [no log yet]'
        print(f'log:      {log_path}{mark}')
        if jobids is not None:
            index = f', array index {array_index}' if array_index is not None else ''
            print(f'slurm:    job {",".join(jobids)} (last submission{index})')
        if data['exception']:
            print(f'\n{data["exception"].rstrip()}')

    def remake_task_log(self, args):
        rmk = self._load(args)
        task = _select_task(rmk, args)
        log_path = _task_log_path(task)
        if args.path:
            print(log_path)
            return
        if not log_path.exists():
            raise RemakeError(
                f'No log for {task} at {log_path} — '
                f'it has not been run via run-task/run-array-task'
            )
        print(log_path.read_text(), end='')

    def remake_why(self, args):
        rmk = self._load(args)
        task = _select_task(rmk, args)
        will_run, reasons = rmk.explain_task(task)
        print(f'{task}  {task.key}')
        print(f'will run: {"yes" if will_run else "no"}')
        if not reasons:
            print('up to date: recorded success, code and uses unchanged, no upstream reruns')
        for reason in reasons:
            print(f'- {reason}')

    def remake_slurm_status(self, args):
        import json

        from .executors.slurm_executor import squeue_snapshot

        rmk = self._load(args)
        snapshot = squeue_snapshot()
        rows = []
        for rule in rmk.rules:
            jobids, _ = _slurm_submission(rule.name)
            if jobids is None:
                continue
            for jobid in jobids:
                elements = snapshot.get(jobid, [])
                states = Counter(state for _, state, _ in elements)
                reasons = sorted(
                    {reason for *_, reason in elements if reason and reason != 'None'}
                )
                rows.append(
                    {
                        'rule': rule.name,
                        'jobid': jobid,
                        'states': dict(states),
                        'reasons': reasons,
                    }
                )
        if args.json:
            print(json.dumps(rows, indent=1))
            return
        if not rows:
            print('No SLURM submissions recorded (.remake/jobs/*.jobids.json)')
            return
        for row in rows:
            states = (
                ' '.join(f'{k}:{v}' for k, v in sorted(row['states'].items()))
                or 'not in queue'
            )
            reasons = f'  [{", ".join(row["reasons"])}]' if row['reasons'] else ''
            print(f'{row["rule"]:<20} job {row["jobid"]:<12} {states}{reasons}')

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

    # Logs go to stderr; stdout carries command output only (so --json and
    # piping stay clean).
    logger.remove()
    if args.debug:
        logger.add(sys.stderr, colorize=True, level='DEBUG')
    elif args.warning:
        logger.add(
            sys.stderr, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='WARNING'
        )
    else:
        logger.add(
            sys.stderr, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='INFO'
        )

    # Per-task-process subcommands (SLURM array elements) get a per-task log
    # sink instead — concurrent appends to the shared log corrupt it on
    # NFS-class filesystems (see design_docs/per_task_logging.md).
    per_task = args.subcmd_name in ('run-task', 'run-array-task')
    if hasattr(args, 'remakefile') and not per_task:
        # Always-on DEBUG file log next to the metadata DB.
        logfile = Path('.remake/remake.log')
        logfile.parent.mkdir(parents=True, exist_ok=True)
        logger.add(logfile, level='DEBUG', rotation='5 MB', retention=3)
        logger.debug(f'argv: {argv}')

    if args.debug_exception:
        # Handle top level exceptions with a debugger.
        sys.excepthook = exception_info

    return parser.dispatch()


if __name__ == '__main__':
    remake_cmd()
