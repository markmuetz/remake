"""remake command line tool.

Minimal surface: run, run-task, info, version. Declarative arg definitions
and method-name dispatch carried over from remake2.
"""
import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

from loguru import logger

from .core import RemakeError
from .loader import load_remake
from .metadata import STATUS_NAMES
from .util import Arg, MutuallyExclusiveGroup, add_argset, task_log_path as _task_log_path
from .version import __version__

# Heavy/optional imports (networkx, the executor stack, squeue) stay local to
# the methods that need them so `remake version`/`task-log` don't pay to import
# them; cheap stdlib (json, ...) is hoisted where it is used widely.

_TB_FRAME = re.compile(r'  File "(.+?)", line (\d+), in (.+)')


def _traceback_signature(tb):
    """A message-insensitive fingerprint of a stored traceback:
    (exception type, frame locations). Tasks failing the same way at the same
    place share a signature even when the message embeds kwargs (`i=0/1/...`),
    so `info -F` can collapse "one bug, N tasks" into a single group."""
    frames = tuple(_TB_FRAME.findall(tb or ''))
    lines = (tb or '').strip().splitlines()
    # Final line is `ExceptionType: message` (or bare `ExceptionType`).
    exc_type = lines[-1].split(':', 1)[0].strip() if lines else ''
    return exc_type, frames


def _group_failures(failures):
    """Collapse failed-task records into unique-signature groups (preserving
    first-seen order). Each group: exc_type, location (the deepest frame),
    count, a representative example (first member, full traceback kept) and
    the member task names. The dedup that makes a wide-array `info -F`
    readable -- one bug across N tasks becomes one group with count N."""
    groups = {}
    for f in failures:
        sig = _traceback_signature(f['exception'])
        groups.setdefault(sig, []).append(f)
    out = []
    for (exc_type, frames), members in groups.items():
        last = frames[-1] if frames else None
        location = f'{last[0]}:{last[1]} in {last[2]}' if last else '(no frames)'
        out.append({
            # MM: this is neat syntax.
            'exc_type': exc_type or '(unknown)',
            'location': location,
            'count': len(members),
            'example': members[0],
            'members': [m['task'] for m in members],
        })
    return out


def _add_task_log_sink(task):
    """One process, one file — safe under concurrent SLURM array elements,
    unlike the shared log."""
    logfile = _task_log_path(task)
    logfile.parent.mkdir(parents=True, exist_ok=True)
    logger.add(logfile, level='DEBUG', mode='w')


def _make_executor(name, rmk, nproc=None):
    """Resolve an executor: a builtin name, or a user class given as a
    dotted path ('mymodule:MyExecutor' or 'mymodule.MyExecutor')."""
    import importlib

    from .executors import (
        DaskExecutor,
        Executor,
        MultiprocExecutor,
        SingleprocExecutor,
        SlurmExecutor,
    )

    if name == 'multiproc':
        return MultiprocExecutor(rmk, nproc=nproc)
    if name == 'dask':
        return DaskExecutor(rmk, nproc=nproc)
    builtin = {'singleproc': SingleprocExecutor, 'slurm': SlurmExecutor}
    if name in builtin:
        return builtin[name](rmk)

    if ':' in name:
        module_name, _, cls_name = name.partition(':')
    elif '.' in name:
        module_name, _, cls_name = name.rpartition('.')
    else:
        raise RemakeError(
            f'Unknown executor {name!r}: use one of '
            f"{sorted([*builtin, 'multiproc', 'dask'])} or a "
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
    """Command line args and dispatch.

    Responsible for handling arguments and dispatching to a Remake instance."""

    # Top level args, i.e. `remake --trace...`
    args = [
        MutuallyExclusiveGroup(
            Arg('--trace', '-T', help='Enable trace logging (most verbose)', action='store_true'),
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true', default=True),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
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
                Arg('--ignore-code-changes',
                    help='Run only tasks that have never succeeded (skip code/uses '
                         'change detection; upstream reruns still propagate)',
                    action='store_true'),
                Arg('--dry-run', '-n', help='Show what would run, run nothing',
                    action='store_true'),
                Arg('--check-outputs', help='Verify outputs of completed tasks (always mode)',
                    action='store_true'),
                Arg('--debug-exception', '-X',
                    help='Run tasks in-process (forces singleproc) and launch '
                         'pdb/ipdb on the first task failure',
                    action='store_true'),
                Arg('--raise', dest='do_raise',
                    help='Re-raise the first task failure with its traceback '
                         '(forces singleproc); unlike -X, does not attach a '
                         'debugger',
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
        'set-state': {
            'help': "Set tasks' recorded state by query, without running them",
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', required=True,
                    help='Tasks to affect (required; use -Q True for all)'),
                Arg('--success', action='store_true',
                    help='Record success (with current code/uses hashes)'),
                Arg('--pending', action='store_true',
                    help='Delete records: tasks become never-run and rerun at '
                         'the next plan (the default check_outputs=never mode '
                         'does not re-adopt on-disk outputs)'),
                Arg('--check-outputs', action='store_true',
                    help='With --success: only tasks whose outputs are complete '
                         'on disk (the explicit adoption path for migrating an '
                         'existing output tree)'),
                Arg('--no-cascade', action='store_true',
                    help='With --success: stamp only the selected tasks. By '
                         'default success also re-stamps downstream complete '
                         'tasks so they are not left looking stale (a guard '
                         'skips any descendant with an independently-newer '
                         'upstream)'),
                Arg('--dry-run', '-n', help='Show affected tasks, change nothing',
                    action='store_true'),
            ],
        },
        'info': {
            'help': 'Per-rule summary of task statuses',
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', help='Filter tasks based on a kwargs query'),
                Arg('--tasks', '-t', help='List individual tasks with status',
                    action='store_true'),
                Arg('--show-failures', '-F',
                    help='Show failures grouped by unique traceback signature + count',
                    action='store_true'),
                Arg('--all-failures',
                    help='With -F, show every failed task individually (not grouped)',
                    action='store_true'),
                Arg('--reasons',
                    help='Per-rule tally of why the to-run tasks would rerun',
                    action='store_true'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'ls-tasks': {
            'help': 'List tasks (key prefix + name), materialising the matrices',
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', help='Filter tasks based on a kwargs query'),
                Arg('--inputs', '-i', action='store_true',
                    help='Show each task\'s input files (indented under it)'),
                Arg('--outputs', '-o', action='store_true',
                    help='Show each task\'s output files (indented under it)'),
                Arg('--check', action='store_true',
                    help='With -i/-o, stat each file and mark exists/complete '
                         '(one stat per file — slow for large selections)'),
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
        'rule-dag': {
            'help': 'Print the rule dependency DAG in topological order '
                    '(rule -> dependent rules)',
            'args': [
                Arg('remakefile'),
                Arg('--number-of-tasks', '-N', action='store_true',
                    help="Annotate each rule with its task count as rule[N] "
                         "(? when a dynamic matrix isn't resolvable yet)"),
                Arg('--matrix-keys', '-M', action='store_true',
                    help='Annotate each rule with its matrix keys as rule(m1, m2)'),
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
            'help': 'Explain why task(s) would (or would not) rerun',
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q',
                    help='Explain all tasks matching this kwargs query; '
                         'omit both key and query to explain the runnable set'),
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
        # -X and --raise both run tasks in this process so the first failure
        # propagates with its original traceback instead of being recorded-
        # and-continued; -X additionally attaches the pdb/ipdb excepthook
        # (wired in remake_cmd, gated on debug_exception). Out-of-process
        # executors can't do either, so force singleproc.
        raise_first = args.debug_exception or args.do_raise
        if raise_first and args.executor != 'singleproc':
            flag = '-X/--debug-exception' if args.debug_exception else '--raise'
            logger.warning(
                f'{flag} runs tasks in-process; '
                f'ignoring --executor {args.executor}'
            )
            executor = _make_executor('singleproc', rmk, nproc=args.nproc)
        else:
            executor = _make_executor(args.executor, rmk, nproc=args.nproc)
        executor.raise_on_failure = raise_first
        if args.dry_run:
            if executor.supports_dry_run:
                executor.dry_run = True
            else:
                runnable, deferred = rmk.plan(
                    query=args.query,
                    force=args.force,
                    ignore_code_changes=args.ignore_code_changes,
                )
                for task in runnable:
                    print(task)
                print(f'{len(runnable)} task(s) would run')
                for rule in deferred:
                    print(f'{rule.name}: deferred (matrix not ready)')
                return
        nfailed = rmk.run(
            executor=executor,
            query=args.query,
            force=args.force,
            ignore_code_changes=args.ignore_code_changes,
        )
        return 1 if nfailed else 0

    def remake_run_task(self, args):
        rmk = self._load(args)
        task = rmk.task_from_key(args.task_key)
        _add_task_log_sink(task)
        logger.info(f'Running {task}')
        rmk.run_task(task)

    def remake_run_array_task(self, args):
        from .metadata.sidecar import SidecarWriter

        # Hundreds of concurrent array elements must not touch the shared
        # SQLite DB (livelock on shared filesystems): load without
        # finalizing (no ensure_rules, no DB connection) and record the
        # result as a sidecar file, ingested by the next plan/info.
        rmk = load_remake(args.remakefile, finalize=False)
        specs = json.loads(Path(f'.remake/jobs/{args.rule}.json').read_text())
        spec = specs[args.index]
        # run_seq was fixed at submission; carry it into the sidecar so its
        # stamp matches the rest of this submission's tasks (older job specs
        # without the field fall back to None — durable check just won't fire).
        rmk.metadata = SidecarWriter(run_seq=spec.get('run_seq'))
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

    def remake_set_state(self, args):
        # Flag validation that is CLI-specific (the --no-cascade spelling)
        # stays here; the rest is Remake.set_state.
        if args.no_cascade and not args.success:
            raise RemakeError('--no-cascade only applies to --success')
        rmk = self._load(args)
        result = rmk.set_state(
            args.query,
            success=args.success,
            pending=args.pending,
            check_outputs=args.check_outputs,
            cascade=not args.no_cascade,
            dry_run=args.dry_run,
        )
        state, tasks, cascaded = result['state'], result['tasks'], result['cascaded']
        skipped = result['skipped']
        for task in tasks:
            print(f'{task} -> {state}')
        for task in cascaded:
            print(f'{task} -> {state} (cascade)')
        suffix = f' ({skipped} skipped: outputs missing/incomplete)' if skipped else ''
        cas = f', {len(cascaded)} cascaded' if cascaded else ''
        verb = 'would be set' if args.dry_run else 'set'
        print(f'{len(tasks)} task(s) {verb} to {state}{cas}{suffix}')

    def remake_info(self, args):
        rmk = self._load(args)
        show_failures = args.show_failures or args.all_failures
        # Remake.status_summary does the gathering; this method only renders
        # (table / --json) and groups failures for display.
        summary = rmk.status_summary(
            query=args.query,
            reasons=args.reasons,
            list_tasks=args.tasks,
            list_failures=show_failures,
        )
        rule_rows = summary['rules']
        totals = summary['totals']
        task_rows = summary.get('tasks', [])
        failures = summary.get('failures', [])

        # Default -F groups failures by traceback signature; --all-failures
        # keeps the exhaustive per-task dump.
        grouped = None if args.all_failures else _group_failures(failures)

        if args.json:
            data = {'rules': rule_rows, 'totals': totals}
            if args.tasks:
                data['tasks'] = task_rows
            if show_failures:
                data['failures'] = failures if args.all_failures else grouped
            print(json.dumps(data, indent=1))
            return

        header = ('rule', 'tasks', 'success', 'failed', 'pending', 'to run')
        rows = [
            (r['rule'], '?', '?', '?', '?', 'deferred')
            if r['deferred']
            else (r['rule'], r['tasks'], r['success'], r['failed'], r['pending'], r['to_run'])
            for r in rule_rows
        ]
        totals_row = (
            'TOTAL', totals['tasks'], totals['success'], totals['failed'],
            totals['pending'], totals['to_run'],
        )
        widths = [
            max(len(str(r[i])) for r in rows + [header, totals_row])
            for i in range(len(header))
        ]
        for row in [header] + rows:
            print('  '.join(f'{str(v):<{w}}' for v, w in zip(row, widths)))
        print('  '.join(f'{str(v):<{w}}' for v, w in zip(totals_row, widths)))
        if args.reasons:
            for r in rule_rows:
                rsn = r.get('reasons')
                if rsn:
                    tally = ', '.join(f'{n} {cat}' for cat, n in sorted(rsn.items()))
                    print(f'  {r["rule"]}: {tally}')
        for task_row in task_rows:
            print(f'{task_row["status"]:<8} {task_row["task"]}')
        if args.all_failures:
            for failure in failures:
                print(f'\n=== {failure["task"]} (failed at {failure["timestamp"]}) ===')
                print(f'log: {failure["log"]}')
                print(failure['exception'].rstrip() or '(no stored exception)')
        elif show_failures:
            for grp in grouped:
                print(f'\n=== {grp["exc_type"]} at {grp["location"]}  '
                      f'×{grp["count"]} ===')
                rep = grp['example']
                print(f'example: {rep["task"]} (failed at {rep["timestamp"]})')
                print(f'log: {rep["log"]}')
                print(rep['exception'].rstrip() or '(no stored exception)')
                others = grp['members'][1:]
                if others:
                    shown = ', '.join(others[:5])
                    more = f' (+{len(others) - 5} more)' if len(others) > 5 else ''
                    print(f'+ {len(others)} more: {shown}{more}')

    def remake_ls_tasks(self, args):
        from .core.dag import iter_expand_rule
        from .core.exceptions import Defer
        from .core.planner import make_predicate

        rmk = self._load(args)
        rmk.finalize()
        predicate = make_predicate(args.query) if args.query else None

        def input_files(task):
            for name, value in task.inputs.items():
                info = {'name': name, 'path': str(value)}
                if args.check:
                    info['exists'] = Path(value).exists()
                yield info

        def output_files(task):
            for name, token in task.outputs.items():
                info = {'name': name, 'path': str(token)}
                if args.check:
                    info['complete'] = token.is_complete()
                yield info

        rows = []
        for rule in rmk.rules:
            try:
                # Stream in text mode: constant memory however big the matrix.
                for task in iter_expand_rule(rule, predicate):
                    if args.json:
                        row = {'key': task.key, 'rule': rule.name, 'kwargs': task.kwargs}
                        if args.inputs:
                            row['inputs'] = list(input_files(task))
                        if args.outputs:
                            row['outputs'] = list(output_files(task))
                        rows.append(row)
                        continue
                    print(task)
                    if args.inputs:
                        for f in input_files(task):
                            mark = '' if not args.check else (
                                ' [exists]' if f['exists'] else ' [MISSING]')
                            print(f'  in  {f["name"]}: {f["path"]}{mark}')
                    if args.outputs:
                        for f in output_files(task):
                            mark = '' if not args.check else (
                                ' [complete]' if f['complete'] else ' [missing]')
                            print(f'  out {f["name"]}: {f["path"]}{mark}')
            except Defer:
                logger.warning(f'{rule.name}: deferred (matrix not ready), tasks unknown')
        if args.json:
            print(json.dumps(rows, indent=1))

    def remake_lint(self, args):
        """Wiring check: every input of a dependent rule should be produced
        by one of its depends_on rules. Flags near-miss paths (typos in
        format strings, off-by-one kwargs) and produced-but-undeclared
        dependencies. Materialises all tasks."""
        import difflib

        from .core.dag import iter_expand_rule
        from .core.exceptions import Defer

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
            except Defer:
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

    def remake_rule_dag(self, args):
        """Print the rule DAG in topological order, one line per rule:
        `rule -> dependent, dependent` (the rules that depend on it).
        Rules with no dependents print as a bare name. -N/-M annotate each
        rule with its task count and/or matrix keys as `rule[N](m1, m2)`;
        N (and the keys) show as ? when a dynamic matrix can't be resolved
        yet (e.g. a continuation rule awaiting upstream outputs)."""
        import networkx as nx

        from .core.dag import build_rule_dag, resolve_matrix
        from .core.exceptions import Defer

        rmk = load_remake(args.remakefile)
        dag = build_rule_dag(rmk.rules)
        order = list(nx.topological_sort(dag))
        pos = {rule: i for i, rule in enumerate(order)}
        edges = {
            rule.name: [s.name for s in sorted(dag.successors(rule), key=pos.get)]
            for rule in order
        }

        need_matrix = args.number_of_tasks or args.matrix_keys
        matrix_info = {}  # rule name -> (n_tasks or None, keys or None)
        if need_matrix:
            for rule in order:
                try:
                    rows = resolve_matrix(rule.matrix)
                except Defer:
                    matrix_info[rule.name] = (None, None)
                    continue
                keys = []
                for row in rows:
                    keys.extend(k for k in row if k not in keys)
                matrix_info[rule.name] = (len(rows), keys)

        if args.json:
            data = {'order': [r.name for r in order], 'edges': edges}
            if need_matrix:
                data['rules'] = {
                    name: {'n_tasks': n, 'matrix_keys': keys}
                    for name, (n, keys) in matrix_info.items()
                }
            print(json.dumps(data, indent=1))
            return

        def label(name):
            s = name
            n, keys = matrix_info.get(name, (None, None))
            if args.number_of_tasks:
                s += f'[{"?" if n is None else n}]'
            if args.matrix_keys:
                s += f'({"?" if keys is None else ", ".join(keys)})'
            return s

        for rule in order:
            succs = edges[rule.name]
            line = label(rule.name)
            if succs:
                line += ' -> ' + ', '.join(succs)
            print(line)

    def remake_task_info(self, args):
        from .executors.slurm_executor import last_submission

        rmk = self._load(args)
        task = rmk.select_task(args.task_key, args.query)
        record = rmk.metadata.get_tasks_status([task]).get(task.key)
        log_path = _task_log_path(task)
        jobids, array_index = last_submission(task.rule.name, task.key)
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
        task = rmk.select_task(args.task_key, args.query)
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
        # One task by key; all matches for a -Q query; the runnable set when
        # neither is given. The query/runnable cases share a single plan()
        # (explain_tasks), so explaining many tasks is plan-cost, not N*plan.
        if args.task_key and args.query:
            raise RemakeError('Give a task key or a -Q query, not both')
        if args.task_key:
            tasks = [rmk.task_from_key(args.task_key)]
        elif args.query:
            tasks = rmk.tasks(query=args.query)
            if not tasks:
                raise RemakeError(f'No task matches {args.query!r}')
        else:
            tasks = None  # default: explain the runnable set

        results = list(rmk.explain_tasks(tasks))
        if not results:
            print('nothing would run: all tasks are up to date')
            return

        n_run = 0
        for task, will_run, reasons in results:
            n_run += will_run
            print(f'{task}  {task.key}')
            print(f'will run: {"yes" if will_run else "no"}')
            if not reasons:
                print('up to date: recorded success, code and uses unchanged, no upstream reruns')
            for reason in reasons:
                print(f'- {reason.message}')
            print()
        if len(results) > 1:
            print(f'{len(results)} task(s): {n_run} would run, '
                  f'{len(results) - n_run} up to date')

    def remake_slurm_status(self, args):
        from .executors.slurm_executor import last_submission, squeue_snapshot

        rmk = self._load(args)
        snapshot = squeue_snapshot()
        rows = []
        for rule in rmk.rules:
            jobids, _ = last_submission(rule.name)
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
    if args.trace:
        logger.add(sys.stderr, colorize=True, level='TRACE')
    elif args.debug:
        logger.add(sys.stderr, colorize=True, level='DEBUG')
    elif args.warning:
        logger.add(
            sys.stderr, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='WARNING'
        )
    else:
        logger.add(
            sys.stderr, colorize=True, format='<bold><lvl>{message}</lvl></bold>', level='INFO'
        )

    # Anchor execution to the remakefile's directory: cd there so .remake/ and
    # the pipeline's relative paths resolve next to the remakefile, not wherever
    # the command happened to be invoked (todos.md). The embedded remakefile arg
    # becomes a bare name, so the SLURM scripts (which re-invoke `remake
    # run-array-task <remakefile>` from the submit dir) stay consistent.
    # Restored after dispatch so in-process/library callers don't see cwd move.
    orig_cwd = os.getcwd()
    if getattr(args, 'remakefile', None):
        rf = Path(args.remakefile).expanduser()
        if str(rf.parent) not in ('', '.'):
            try:
                os.chdir(rf.parent)
            except OSError as e:
                print(f'error: cannot enter remakefile directory {rf.parent}: {e}',
                      file=sys.stderr)
                return 1
        args.remakefile = rf.name

    # Per-task-process subcommands (SLURM array elements) get a per-task log
    # sink instead — concurrent appends to the shared log corrupt it on
    # NFS-class filesystems (see design_docs/per_task_logging.md).
    per_task = args.subcmd_name in ('run-task', 'run-array-task')
    if hasattr(args, 'remakefile') and not per_task:
        # Always-on file log next to the metadata DB (DEBUG, or TRACE under -T).
        logfile = Path('.remake/remake.log')
        logfile.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            logfile, level='TRACE' if args.trace else 'DEBUG',
            rotation='5 MB', retention=3,
        )
        logger.debug(f'argv: {argv}')

    if getattr(args, 'debug_exception', False):
        # Handle top level exceptions with a debugger (run -X only).
        sys.excepthook = exception_info

    try:
        return parser.dispatch()
    except RemakeError as e:
        # User-facing errors (bad query, >1-task match, unknown rule, ...)
        # print cleanly and exit 2; keep the traceback only under -X.
        if getattr(args, 'debug_exception', False):
            raise
        print(f'error: {e}', file=sys.stderr)
        return 2
    finally:
        os.chdir(orig_cwd)


if __name__ == '__main__':
    remake_cmd()
