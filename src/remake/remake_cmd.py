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
from .util import (
    Arg,
    MutuallyExclusiveGroup,
    Painter,
    add_argset,
    task_log_path as _task_log_path,
)
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


def _format_bytes(nbytes):
    """Bytes as a short human-readable string (1024-based)."""
    value = float(nbytes)
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if value < 1024 or unit == 'T':
            return f'{value:.0f}{unit}' if unit == 'B' else f'{value:.1f}{unit}'
        value /= 1024


def _resources_line(resources):
    """The `resources:` line for `task-info`, or None if nothing was
    measured. Peak RSS is annotated when it did not come from sampling, so a
    getrusage number (interpreter baseline included) is not read as a
    like-for-like measurement — see design_docs/resource_capture.md."""
    if resources.get('wall_s') is None:
        return None
    parts = [f'wall {resources["wall_s"]:.2f}s']
    if resources.get('cpu_s') is not None:
        parts.append(f'cpu {resources["cpu_s"]:.2f}s')
    if resources.get('max_rss_bytes') is not None:
        peak = f'peak rss {_format_bytes(resources["max_rss_bytes"])}'
        if resources.get('rss_method') != 'sample':
            peak += f' ({resources["rss_method"]})'
        parts.append(peak)
    return ', '.join(parts)


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


class RemakeCLI:
    """Command line args and dispatch.

    Responsible for handling arguments and dispatching to a Remake instance,
    and then rendering the results to terminal."""

    # Top level args, i.e. `remake --trace...`
    args = [
        MutuallyExclusiveGroup(
            Arg('--trace', '-T', help='Enable trace logging (most verbose)', action='store_true'),
            Arg('--debug', '-D', help='Enable debug logging', action='store_true'),
            Arg('--info', '-I', help='Enable info logging', action='store_true', default=True),
            Arg('--warning', '-W', help='Warning logging only', action='store_true'),
        ),
        Arg('--colour', '--color', dest='colour', choices=['auto', 'always', 'never'],
            default='auto',
            help='Colourise output: auto (TTY only, the default), always, or '
                 'never. NO_COLOR/FORCE_COLOR env vars are honoured under auto.'),
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
                Arg('--specs', default=None,
                    help="Job-spec file to read (defaults to the rule's "
                         'last submission)'),
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
        'rule-info': {
            'help': 'Detail view of one rule: docstring, matrix, input/output '
                    'templates, uses',
            'args': [
                Arg('remakefile'),
                Arg('rule_name'),
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
        for argset in RemakeCLI.args:
            add_argset(parser, argset)

        subparsers = parser.add_subparsers(dest='subcmd_name')
        for cmd_key, cmd_kwargs in RemakeCLI.sub_cmds.items():
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
        from .util.resources import one_task_per_process

        rmk = self._load(args)
        task = rmk.task_from_key(args.task_key)
        _add_task_log_sink(task)
        logger.info(f'Running {task}')
        # One task, then the process exits: getrusage's process-wide peak RSS
        # is this task's peak, so it is a valid fallback where /proc is
        # unavailable (util/resources.py).
        with one_task_per_process():
            rmk.run_task(task)

    def remake_run_array_task(self, args):
        from .executors.slurm_executor import submitted_spec_path
        from .metadata.sidecar import SidecarWriter
        from .util.resources import one_task_per_process

        # Hundreds of concurrent array elements must not touch the shared
        # SQLite DB (livelock on shared filesystems): load without
        # finalizing (no ensure_rules, no DB connection) and record the
        # result as a sidecar file, ingested by the next plan/info.
        rmk = load_remake(args.remakefile, finalize=False)
        # Generated sbatch scripts pin their submission's spec file via
        # --specs; the fallback (manual retries, in-flight jobs submitted by
        # pre-0.9 scripts) must resolve the last SUBMITTED spec — a dry run
        # writes a newer spec file that no job is running.
        specs_path = Path(args.specs) if args.specs else submitted_spec_path(args.rule)
        if specs_path is None or not specs_path.exists():
            raise RemakeError(
                f'No job specs for rule {args.rule} — generate them with: '
                f'remake run {args.remakefile} --executor slurm'
            )
        specs = json.loads(specs_path.read_text())
        spec = specs[args.index]
        # run_seq was fixed at submission; carry it into the sidecar so its
        # stamp matches the rest of this submission's tasks (older job specs
        # without the field fall back to None — durable check just won't fire).
        rmk.metadata = SidecarWriter(run_seq=spec.get('run_seq'))
        task = rmk.task_from_spec(spec['rule'], spec['kwargs'])
        if task.key != spec['task_key']:
            # Kwargs didn't survive the JSON round-trip (should have been
            # caught at spec-write time): running would record the result
            # under a key the planner never reads — pending forever.
            raise RemakeError(
                f'{args.rule}[{args.index}]: rebuilt task key {task.key} != '
                f'submitted key {spec["task_key"]} — kwargs changed in the '
                f'JSON round-trip through {specs_path}'
            )
        _add_task_log_sink(task)
        logger.info(f'Running {task}')
        # As run-task: one array element = one task = one process.
        with one_task_per_process():
            rmk.run_task(task)

    def remake_resubmit(self, args):
        import subprocess as sp

        from .executors.slurm_executor import check_resubmit_safe

        submit = Path('.remake/submit.sh')
        if not submit.exists():
            raise RemakeError(
                f'No {submit} — generate it with: remake run {args.remakefile} --executor slurm'
            )
        check_resubmit_safe(submit)
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

        paint = Painter(args.colour)

        # Four-state partition + the plan's view: up-to-date + stale + failed
        # + pending == tasks, and up-to-date + to-run == tasks.
        header = ('rule', 'tasks', 'up-to-date', 'stale', 'failed', 'pending', 'to run')
        rows = [
            (r['rule'], '?', '?', '?', '?', '?', 'deferred')
            if r['deferred']
            else (r['rule'], r['tasks'], r['up_to_date'], r['stale'],
                  r['failed'], r['pending'], r['to_run'])
            for r in rule_rows
        ]
        totals_row = (
            'TOTAL', totals['tasks'], totals['up_to_date'], totals['stale'],
            totals['failed'], totals['pending'], totals['to_run'],
        )
        widths = [
            max(len(str(r[i])) for r in rows + [header, totals_row])
            for i in range(len(header))
        ]

        # Per-column styling for the count columns (rule name + tasks count are
        # left plain). A zero count is dimmed so non-zero cells stand out.
        col_style = (None, None, ('green',), ('cyan',), ('red', 'bold'),
                     ('yellow',), ('cyan',))

        def render_row(row, *, emphasise=False):
            cells = []
            for i, (v, w) in enumerate(zip(row, widths)):
                cell = f'{str(v):<{w}}'  # pad on raw text, colour after
                style = col_style[i]
                if style is not None and str(v) not in ('0', '?'):
                    cell = paint(cell, *style)
                elif style is not None and str(v) == '0':
                    cell = paint(cell, 'dim')
                if emphasise and i == 0:
                    cell = paint(cell, 'bold')
                cells.append(cell)
            return '  '.join(cells)

        # Header row stays uncoloured (it labels the colours below it).
        print('  '.join(f'{str(v):<{w}}' for v, w in zip(header, widths)))
        for row in rows:
            print(render_row(row))
        print(render_row(totals_row, emphasise=True))
        if args.reasons:
            for r in rule_rows:
                rsn = r.get('reasons')
                if rsn:
                    tally = ', '.join(f'{n} {cat}' for cat, n in sorted(rsn.items()))
                    print(f'  {r["rule"]}: {tally}')
        for task_row in task_rows:
            status = task_row['status']
            print(f'{paint.status(status, f"{status:<8}")} {task_row["task"]}')
        if args.all_failures:
            for failure in failures:
                print(paint(f'\n=== {failure["task"]} (failed at '
                            f'{failure["timestamp"]}) ===', 'red', 'bold'))
                print(f'log: {failure["log"]}')
                print(failure['exception'].rstrip() or '(no stored exception)')
        elif show_failures:
            for grp in grouped:
                print(paint(f'\n=== {grp["exc_type"]} at {grp["location"]}  '
                            f'×{grp["count"]} ===', 'red', 'bold'))
                rep = grp['example']
                print(f'example: {rep["task"]} (failed at {rep["timestamp"]})')
                print(f'log: {rep["log"]}')
                print(rep['exception'].rstrip() or '(no stored exception)')
                others = grp['members'][1:]
                if others:
                    shown = '\n'.join(others[:5])
                    more = f' (+{len(others) - 5} more)' if len(others) > 5 else ''
                    print(f'+ {len(others)} more:\n{shown}{more}')

    def remake_ls_tasks(self, args):
        from .core.dag import iter_expand_rule
        from .core.exceptions import Defer
        from .core.planner import make_predicate

        rmk = self._load(args)
        rmk.finalize()
        predicate = make_predicate(args.query) if args.query else None
        paint = Painter(args.colour)

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
                                paint(' [exists]', 'green') if f['exists']
                                else paint(' [missing]', 'red', 'bold'))
                            print(f'  in  {f["name"]}: {f["path"]}{mark}')
                    if args.outputs:
                        for f in output_files(task):
                            mark = '' if not args.check else (
                                paint(' [complete]', 'green') if f['complete']
                                else paint(' [missing]', 'red', 'bold'))
                            print(f'  out {f["name"]}: {f["path"]}{mark}')
            except Defer:
                logger.warning(f'{rule.name}: deferred (matrix not ready), tasks unknown')
        if args.json:
            print(json.dumps(rows, indent=1))

    def remake_lint(self, args):
        rmk = self._load(args)
        rows = rmk.lint()
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
        rmk = load_remake(args.remakefile)
        need_matrix = args.number_of_tasks or args.matrix_keys
        info = rmk.rule_dag(with_matrix=need_matrix)
        order, edges = info['order'], info['edges']
        matrix_info = info.get('matrix_info', {})

        if args.json:
            data = {'order': order, 'edges': edges}
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

        for name in order:
            line = label(name)
            if edges[name]:
                line += ' -> ' + ', '.join(edges[name])
            print(line)

    def remake_rule_info(self, args):
        rmk = load_remake(args.remakefile)
        rule = rmk.rule_from_name(args.rule_name)
        data = rmk.rule_info(rule)
        if args.json:
            print(json.dumps(data, indent=1))
            return

        paint = Painter(args.colour)
        print(paint(data['rule'], 'cyan', 'bold'))
        if data['docstring']:
            for line in data['docstring'].splitlines():
                print(f'  {line}')
        print()
        if data['depends_on']:
            print(f'depends on:  {", ".join(data["depends_on"])}')
        if data['dependents']:
            print(f'dependents:  {", ".join(data["dependents"])}')

        m = data['matrix']
        if m['kind'] == 'none':
            print('matrix:      (none: one task)')
        elif m['n_tasks'] is None:
            mark = ' (@deferrable)' if m['deferrable'] else ''
            print(f'matrix:      dynamic{mark} — not resolvable yet '
                  f'(upstream outputs missing)')
        else:
            keys = ', '.join(m['keys'])
            print(f'matrix:      ({keys}) — {m["n_tasks"]} task(s)')
            for key, values in (m['values'] or {}).items():
                print(f'  {key}: {values!r}')

        for part in ('inputs', 'outputs'):
            part_info = data[part]
            if part_info is None:
                continue
            if 'error' in part_info:
                print(f'{part + ":":<12} '
                      + paint(f'templates not derivable ({part_info["error"]})', 'dim'))
                continue
            print(f'{part}:')
            for name, template in part_info['templates'].items():
                print(f'  {template}  ({name})')

        if data['uses']:
            print('uses:')
            for entry in data['uses']:
                if entry['kind'] == 'value':
                    print(f'  {entry["name"]} = {entry["rendering"]}')
                elif entry['kind'] == 'source':
                    print(f'  {entry["name"]}:')
                    for line in entry['rendering'].rstrip().splitlines():
                        print(f'    {line}')
                else:
                    print(f'  {entry["name"]}: '
                          + paint(f'{entry["rendering"]} (source unavailable)', 'dim'))
        if data['config']:
            print(f'config:      {data["config"]!r}')

    def remake_task_info(self, args):
        rmk = self._load(args)
        task = rmk.select_task(args.task_key, args.query)
        data = rmk.task_info(task)
        if args.json:
            print(json.dumps(data, indent=1))
            return

        paint = Painter(args.colour)
        print(f'{task}  {task.key}')
        when = f' at {data["timestamp"]}' if data['timestamp'] else ''
        print(f'status:   {paint.status(data["status"])}{when}')
        resources = _resources_line(data['resources'])
        if resources:
            # Of the last execution — set-state doesn't un-measure a run.
            print(f'resources: {resources}')
        for name, path_info in data['inputs'].items():
            mark = (paint('exists', 'green') if path_info['exists']
                    else paint('missing', 'red', 'bold'))
            print(f'input:    {path_info["path"]}  [{mark}]  ({name})')
        for name, path_info in data['outputs'].items():
            mark = (paint('complete', 'green') if path_info['complete']
                    else paint('missing', 'red', 'bold'))
            print(f'output:   {path_info["path"]}  [{mark}]  ({name})')
        log = data['log']
        mark = '' if log['exists'] else paint('  [no log yet]', 'dim')
        print(f'log:      {log["path"]}{mark}')
        jobids, array_index = data['slurm']['jobids'], data['slurm']['array_index']
        if jobids is not None:
            index = f', array index {array_index}' if array_index is not None else ''
            print(f'slurm:    job {",".join(jobids)} (last submission{index})')
        if data['exception']:
            print(f'\n{paint(data["exception"].rstrip(), "red")}')

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
        results = list(rmk.why(args.task_key, args.query))
        if not results:
            print('nothing would run: all tasks are up to date')
            return

        paint = Painter(args.colour)
        n_run = 0
        for task, will_run, reasons in results:
            n_run += will_run
            print(f'{task}  {task.key}')
            verdict = (paint('yes', 'cyan', 'bold') if will_run
                       else paint('no', 'green'))
            print(f'will run: {verdict}')
            if not reasons:
                print(paint('up to date: recorded success, code and uses '
                            'unchanged, no upstream reruns', 'dim'))
            for reason in reasons:
                print(paint(f'- {reason.message}', 'yellow'))
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
    cli = RemakeCLI()
    args = cli.parse_args(argv)
    if not args.subcmd_name:
        cli.parser.print_help()
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
    # sink instead — concurrent appends to the shared logs corrupt them on
    # NFS-class filesystems (see design_docs/per_task_logging.md).
    per_task = args.subcmd_name in ('run-task', 'run-array-task')
    if hasattr(args, 'remakefile') and not per_task:
        import uuid

        # Every record from this invocation shares one run_id (surfaced in the
        # structured sink), so a miner can group an invocation's lines and
        # correlate e.g. a plan total with its constituent status queries.
        logger.configure(extra={'run_id': uuid.uuid4().hex[:12]})
        Path('.remake').mkdir(parents=True, exist_ok=True)
        debug_level = 'TRACE' if args.trace else 'DEBUG'
        # Three always-on file sinks next to the metadata DB, split so the
        # streams don't compete for one rotation window (logs_analysis §3.2):
        #  - remake.log: the human-facing run narrative (INFO+).
        #  - remake.debug.log: the DEBUG (TRACE under -T) firehose.
        #  - remake.jsonl: structured mirror of the firehose — one JSON object
        #    per record, with bound fields (event=..., ntasks=..., seconds=...)
        #    under record.extra, so mining is jq, not regex (logs_analysis §4).
        logger.add(
            '.remake/remake.log', level='INFO',
            rotation='5 MB', retention=3,
        )
        logger.add(
            '.remake/remake.debug.log', level=debug_level,
            rotation='5 MB', retention=3,
        )
        logger.add(
            '.remake/remake.jsonl', level=debug_level, serialize=True,
            rotation='5 MB', retention=3,
        )
        logger.bind(event='invocation').debug(f'argv: {argv}')

    if getattr(args, 'debug_exception', False):
        # Handle top level exceptions with a debugger (run -X only).
        sys.excepthook = exception_info

    try:
        return cli.dispatch()
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
