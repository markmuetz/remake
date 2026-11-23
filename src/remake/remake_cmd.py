"""remake command line tool.

Minimal surface: run, run-task, info, version. Declarative arg definitions
and method-name dispatch carried over from remake2.
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
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
from .util.config import project_metadata_dir
from .util.colour import STATUS_STYLE
from .util.table import render_table, terminal_width
from .util.paths import log_file, metadata_dir, set_metadata_dir, slurm_output_dir, submit_path
from .version import __version__

# Heavy/optional imports (networkx, the executor stack, squeue) stay local to
# the methods that need them so `remake version`/`task-log` don't pay to import
# them; cheap stdlib (json, ...) is hoisted where it is used widely.

# `remake run` exit codes (docs/guide/running.md; frozen at 1.0). 2 is a
# usage error and 128+N a signal, both handled in main().
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_BLOCKED = 3

_TB_FRAME = re.compile(r'  File "(.+?)", line (\d+), in (.+)')


JSON_FORMAT = 1  # version of the `--json` output shapes; see docs/cli.md


def _print_json(payload):
    """Print `payload` (a dict) as JSON with `"format": JSON_FORMAT` as the
    first key. Every `--json` output goes through here, so the top level is
    always a versioned object, never a bare list."""
    print(json.dumps({'format': JSON_FORMAT, **payload}, indent=1, default=repr))


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


def _is_count(v):
    return isinstance(v, int) and not isinstance(v, bool)


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
    like-for-like measurement — see design_docs/designs/resource_capture.md."""
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


def _last_line(text):
    lines = [line.strip() for line in (text or '').splitlines() if line.strip()]
    return lines[-1] if lines else ''


def _failed_line(failure):
    """One summary dict per failed task for `info --failed`: the last line of
    the stored exception, else of the task's SLURM .err (failures before the
    task starts, e.g. import errors, never reach the DB)."""
    error = _last_line(failure['exception'])
    if not error:
        from .executors.slurm_executor import (_read_sidecar, element_output,
                                               last_submission, pointer_path)

        jobids, index = last_submission(failure['rule'], failure['key'])
        if index is not None:
            legacy = 'v' not in (_read_sidecar(pointer_path(failure['rule'])) or {})
            err = element_output(failure['rule'], (jobids or [None])[0], index, 'err',
                                 legacy=legacy)
            if err.exists():
                error = _last_line(err.read_text(errors='replace'))
    return {
        'rule': failure['rule'],
        'key': failure['key'],
        'error': error or '(no recorded error)',
        'log': failure['log'],
    }


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
    try:
        cls = getattr(importlib.import_module(module_name), cls_name)
    except (ImportError, AttributeError) as e:
        raise RemakeError(f'Cannot load executor {name!r}: {e}') from None
    if not (isinstance(cls, type) and issubclass(cls, Executor)):
        raise RemakeError(f'{name!r} is not an Executor subclass')
    return cls(rmk)


def find_remakefiles(root, recursive=False):
    """*.py files under `root` that contain a `Remake(...)` call, found by an
    AST scan — never imported, so side effects (make_example_data.py) don't
    run. Sorted by path."""
    import ast

    root = Path(root)
    found = []
    for f in sorted(root.rglob('*.py') if recursive else root.glob('*.py')):
        if any(part.startswith('.') or part == '__pycache__'
               for part in f.relative_to(root).parts):
            continue
        try:
            tree = ast.parse(f.read_text())
        except (SyntaxError, ValueError, OSError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                if (isinstance(fn, ast.Name) and fn.id == 'Remake') or (
                        isinstance(fn, ast.Attribute) and fn.attr == 'Remake'):
                    found.append(f)
                    break
    return found


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
        Arg('--profile', default=None, metavar='NAME',
            help='Config profile to use (e.g. jasmin); beats $REMAKE_PROFILE '
                 'and the profile set in config files'),
    ]
    sub_cmds = {
        'config': {
            'help': 'Print the effective merged config and where each key came from',
            'args': [
                Arg('remakefile'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
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
                    help='Record pending: tasks rerun at the next plan (the '
                         'record is kept, with its last measured resources)'),
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
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'verify': {
            'help': 'Reconcile the DB with the outputs on disk (read-only unless --apply)',
            'args': [
                Arg('remakefile'),
                Arg('--query', '-Q', help='Only tasks matching this kwargs query'),
                Arg('--outputs', action='store_true',
                    help='Recorded success, outputs missing or failing Ensure '
                         '(with --apply: mark pending)'),
                Arg('--adopt', action='store_true',
                    help='No record, outputs complete on disk (with --apply: '
                         'record success)'),
                Arg('--apply', action='store_true', help='Write the changes'),
                Arg('--json', help='Machine-readable output', action='store_true'),
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
                Arg('--failed',
                    help='One line per failed task: rule, short key, last line of '
                         'its error, log path (falls back to the SLURM .err)',
                    action='store_true'),
                Arg('--reasons',
                    help='Per-rule tally of why the to-run tasks would rerun',
                    action='store_true'),
                Arg('--sizes',
                    help='Add a column with each rule\'s recorded output size '
                         '(--json always has output_bytes)',
                    action='store_true'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'export-task': {
            'help': "Write a task's fully resolved spec as JSON (to run its body "
                    'without importing remake)',
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q', help='Select the task by kwargs query instead of key'),
                Arg('--output', '-o', help='Write to this file instead of stdout'),
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
        'dag': {
            'help': 'Export the DAG as Graphviz DOT (rule-level by default)',
            'args': [
                Arg('remakefile'),
                MutuallyExclusiveGroup(
                    Arg('--rules', action='store_true',
                        help='Rule-level graph (the default)'),
                    Arg('--tasks', action='store_true',
                        help='One node per task, edges from path-derived '
                             'dependencies; deferred rules are one node'),
                ),
                Arg('--output', '-o', metavar='FILE',
                    help='Write to FILE instead of stdout'),
            ],
        },
        'run-all': {
            'help': 'Run every remakefile found (AST scan, never imported) in a '
                    'directory, in sorted order; no ordering promises',
            'args': [
                Arg('directory', nargs='?', default='.'),
                Arg('--recursive', '-r', action='store_true',
                    help='Also search subdirectories'),
                Arg('--executor', '-E', default='singleproc',
                    help='As for run'),
                Arg('--nproc', '-j', type=int, help='As for run'),
                Arg('--force', '-f', action='store_true', help='As for run'),
                Arg('--ignore-code-changes', action='store_true', help='As for run'),
                Arg('--dry-run', '-n', action='store_true', help='As for run'),
                Arg('--check-outputs', action='store_true', help='As for run'),
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
        'history': {
            'help': "Run history: a task's executions, or recent runs (--runs)",
            'args': [
                Arg('remakefile'),
                Arg('task_key', nargs='?'),
                Arg('--query', '-Q', help='Select the task by kwargs query instead of key'),
                Arg('--runs', help='List recent runs instead, one line each',
                    action='store_true'),
                Arg('--limit', help='Show at most N rows (default 20)', type=int,
                    default=20),
                Arg('--prune', help='Apply the per-task cap (history.keep) to every '
                    'task now', action='store_true'),
                Arg('--keep', help='With --prune: keep N executions per task', type=int),
                Arg('--json', help='Machine-readable output', action='store_true'),
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
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'slurm-status': {
            'help': 'Live SLURM queue state of the last submission, per rule '
                    '(PD pending, R running, from squeue; done/failed from the DB)',
            'args': [
                Arg('remakefile'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        'check-env': {
            'help': 'Check a SLURM job would import the same code as this process',
            'args': [
                Arg('remakefile'),
                Arg('--srun', action='store_true',
                    help='Run the job-side check on a compute node via srun'),
                Arg('--json', help='Machine-readable output', action='store_true'),
            ],
        },
        '_env-report': {
            'help': argparse.SUPPRESS,  # internal: used by check-env
            'args': [
                Arg('remakefile'),
                Arg('--json', action='store_true'),
            ],
        },
        'version': {
            'help': 'Print remake version',
            'args': [],
        },
    }

    def __init__(self):
        self.args = None
        self.loaded = []  # Remakes loaded by this command (see _track)
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
            # Also accepted after the subcommand (`remake info --colour never
            # x.py`); SUPPRESS keeps the top-level value when it's absent.
            subparser.add_argument(
                '--colour', '--color', dest='colour', choices=['auto', 'always', 'never'],
                default=argparse.SUPPRESS,
                help='as `remake --colour`: auto (TTY only), always or never')

        return parser

    def parse_args(self, argv):
        self.args = self.parser.parse_args(argv[1:])
        return self.args

    def dispatch(self):
        args = self.args
        method_name = 'remake_' + args.subcmd_name.replace('-', '_')
        return getattr(self, method_name)(args)

    def _track(self, rmk):
        """Remember a loaded Remake so remake_cmd can close its metadata
        backend when the command ends, rather than leaving the SQLite
        connection to garbage collection (a ResourceWarning, and an
        unclosed DB handle for in-process callers)."""
        self.loaded.append(rmk)
        return rmk

    def _load(self, args):
        rmk = self._track(load_remake(args.remakefile, profile=args.profile))
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
                rmk._check_external_inputs(runnable, dry_run=True)
                from .core.remake import deferred_roots

                roots = deferred_roots(rmk.dag, deferred)
                if deferred:
                    # Same words and layout as `info`'s deferred list.
                    print('\ndeferred, not run:')
                    name_w = max(len(rule.name) for rule in deferred)
                    for rule in deferred:
                        why = (f'held by {", ".join(roots[rule])}' if roots[rule]
                               else 'matrix not ready')
                        print(f'  - {rule.name:<{name_w}}  {why}')
                return
        nfailed = rmk.run(
            executor=executor,
            query=args.query,
            force=args.force,
            ignore_code_changes=args.ignore_code_changes,
        )
        # Exit 0 only when everything needed completed. A failed task is 1;
        # a run that only stopped short (rules left blocked: a deferred
        # matrix never became ready) is 3, so scripts can tell the two apart
        # (0.10 decision D5). A failure wins when both happen.
        if nfailed:
            return EXIT_FAILED
        return EXIT_BLOCKED if rmk.blocked_rules else EXIT_OK

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
        from .executors.slurm_executor import load_job_specs, submitted_spec_path
        from .metadata.sidecar import SidecarWriter
        from .util.resources import one_task_per_process

        # Hundreds of concurrent array elements must not touch the shared
        # SQLite DB (livelock on shared filesystems): load without
        # finalizing (no ensure_rules, no DB connection) and record the
        # result as a sidecar file, ingested by the next plan/info.
        rmk = self._track(load_remake(args.remakefile, finalize=False, profile=args.profile))
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
        specs = load_job_specs(specs_path)
        if not 0 <= args.index < len(specs):
            raise RemakeError(
                f'array index {args.index} out of range: {specs_path} has '
                f'{len(specs)} task(s)'
            )
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
        task.rerun_reason = spec.get('reason')
        rmk.planner_env = spec.get('env')
        _add_task_log_sink(task)
        logger.info(f'Running {task}')
        # As run-task: one array element = one task = one process.
        with one_task_per_process():
            rmk.run_task(task)

    def remake_resubmit(self, args):
        import subprocess as sp

        from .executors.slurm_executor import check_resubmit_safe

        submit = submit_path()
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
        if args.json:
            _print_json({
                'state': state, 'dry_run': bool(args.dry_run), 'skipped': skipped,
                'tasks': [{'task': str(t), 'key': t.key, 'cascade': False} for t in tasks]
                + [{'task': str(t), 'key': t.key, 'cascade': True} for t in cascaded],
            })
            return
        for task in tasks:
            print(f'{task} -> {state}')
        for task in cascaded:
            print(f'{task} -> {state} (cascade)')
        if args.check_outputs:
            print('note: `remake verify --adopt` reports the same tasks, and '
                  '`--apply` records them', file=sys.stderr)
        suffix = f' ({skipped} skipped: outputs missing/incomplete)' if skipped else ''
        cas = f', {len(cascaded)} cascaded' if cascaded else ''
        verb = 'would be set' if args.dry_run else 'set'
        print(f'{len(tasks)} task(s) {verb} to {state}{cas}{suffix}')

    def remake_verify(self, args):
        """`remake verify`: report (and with --apply, fix) DB/disk mismatches.
        Neither --outputs nor --adopt means both. Exit 1 when mismatches
        were found and not applied, else 0."""
        rmk = self._load(args)
        both = not (args.outputs or args.adopt)
        result = rmk.verify(args.query, outputs=args.outputs or both,
                            adopt=args.adopt or both, apply=args.apply)
        missing, adoptable = result['missing'], result['adoptable']
        if args.json:
            def rows(tasks):
                return [{'task': str(t), 'key': t.key, 'rule': t.rule.name} for t in tasks]
            _print_json({'applied': result['applied'], 'missing': rows(missing),
                         'adoptable': rows(adoptable), 'cascaded': rows(result['cascaded'])})
        else:
            paint = Painter(args.colour)
            for t in missing:
                print(f'{t}  {paint("outputs missing", "red")}'
                      + (' -> pending' if args.apply else ''))
            for t in adoptable:
                print(f'{t}  {paint("adoptable", "green")}'
                      + (' -> success' if args.apply else ''))
            for t in result['cascaded']:
                print(f'{t}  -> success (cascade)')
            if not (missing or adoptable):
                # All well: say so, rather than two zeros to decode.
                what = ('the DB matches the outputs on disk' if both else
                        'every recorded success has its outputs' if args.outputs else
                        'no unrecorded task has complete outputs')
                n = result['checked']
                print(f'{what} ({n} task{"" if n == 1 else "s"} checked)')
            else:
                print(f'{len(missing)} recorded success with outputs missing, '
                      f'{len(adoptable)} unrecorded with outputs complete'
                      + ('' if args.apply else '; rerun with --apply to record'))
        return 1 if (missing or adoptable) and not args.apply else 0

    def remake_info(self, args):
        rmk = self._load(args)
        show_failures = args.show_failures or args.all_failures
        # Remake.info does the gathering; this method only renders
        # (table / --json) and groups failures for display.
        summary = rmk.info(
            query=args.query,
            reasons=args.reasons,
            list_tasks=args.tasks,
            list_failures=show_failures or args.failed,
        )
        rule_rows = summary['rules']
        totals = summary['totals']
        task_rows = summary.get('tasks', [])
        failures = summary.get('failures', [])

        # Default -F groups failures by traceback signature; --all-failures
        # keeps the exhaustive per-task dump.
        grouped = None if args.all_failures else _group_failures(failures)

        if args.failed:
            lines = [_failed_line(f) for f in failures]
            if args.json:
                _print_json({'failures': lines})
            else:
                # One line per task (grep-able). Piped, it ends with the log
                # path; in a terminal (where the path would be cut to fit and
                # be useless) a footer says how to get a log from the key.
                width = terminal_width()
                for line in lines:
                    first = f'{line["rule"]}  {line["key"][:8]}  {line["error"]}'
                    print(first if width else f'{first}  [{line["log"]}]')
                if width and lines:
                    paint = Painter(args.colour)
                    print()
                    print(paint(f'logs: remake task-log {args.remakefile} <key>', 'dim'))
            return

        if args.json:
            data = {'rules': rule_rows, 'totals': totals}
            if args.tasks:
                data['tasks'] = task_rows
            if show_failures:
                data['failures'] = failures if args.all_failures else grouped
            _print_json(data)
            return

        paint = Painter(args.colour)

        # Four-state partition + the plan's view: up-to-date + stale + failed
        # + pending == tasks, and up-to-date + to-run == tasks.
        header = ('rule', 'tasks', 'up-to-date', 'stale', 'failed', 'pending', 'to run')
        # A rule deferred only by an upstream rule has known tasks: its count
        # and DB failed/pending show, up-to-date vs stale can't (no plan yet).
        # A deferred rule's to-run is unknown ('?'); lines under TOTAL say why.
        def _row(r):
            if not r['deferred']:
                return (r['rule'], r['tasks'], r['up_to_date'], r['stale'],
                        r['failed'], r['pending'], r['to_run'])
            if 'tasks' in r:
                return (r['rule'], r['tasks'], '?', '?', r['failed'], r['pending'], '?')
            return (r['rule'], '?', '?', '?', '?', '?', '?')

        def _note(r):
            if not r['deferred']:
                return ''
            return (f'held by {", ".join(r["deferred_by"])}' if r['deferred_by']
                    else 'matrix not ready')

        rows = [_row(r) for r in rule_rows]
        totals_row = (
            'TOTAL', totals['tasks'], totals['up_to_date'], totals['stale'],
            totals['failed'], totals['pending'], totals['to_run'],
        )
        # An 'output' size column on request (--sizes): the default table is stable.
        col_style = (None, None, ('green',), ('cyan',), ('red', 'bold'),
                     ('yellow',), ('cyan',))
        if args.sizes:
            header += ('output',)
            col_style += (None,)
            rows = [
                row + ('?' if r['deferred'] else _format_bytes(r.get('output_bytes', 0)),)
                for row, r in zip(rows, rule_rows)
            ]
            totals_row += (_format_bytes(totals['output_bytes']),)
        def styled(style):
            # Colour a count; '?' and words stay plain (zero is dimmed by
            # render_table).
            return lambda v: style if _is_count(v) and v != 0 else None

        styles = [None if st is None else styled(st) for st in col_style]
        for line in render_table(header, rows, total=totals_row, paint=paint,
                                 styles=styles, width=terminal_width()):
            print(line)
        deferred_rows = [r for r in rule_rows if r['deferred']]
        if deferred_rows:
            known = [r for r in deferred_rows if 'tasks' in r]
            unknown = len(deferred_rows) - len(known)
            parts = []
            if known:
                n = totals['deferred_tasks']
                parts.append(f'{n} task{"s" if n != 1 else ""} in {len(known)} '
                             f'held rule{"s" if len(known) != 1 else ""}')
            if unknown:
                parts.append(f'{unknown} rule{"s" if unknown != 1 else ""} with tasks unknown')
            print(paint(f'deferred, not in TOTAL: {"; ".join(parts)}', 'dim'))
            name_w = max(len(r['rule']) for r in deferred_rows)
            for r in deferred_rows:
                print(paint(f'  - {r["rule"]:<{name_w}}  {_note(r)}', 'dim'))
        if args.reasons and any(r.get('reasons') for r in rule_rows):
            print('\nwhy the to-run tasks would run:')
            for r in rule_rows:
                rsn = r.get('reasons')
                if rsn:
                    tally = ', '.join(f'{n} {cat}' for cat, n in sorted(rsn.items()))
                    print(f'  - {r["rule"]}: {tally}')
        if task_rows:
            print()  # the task list is not part of the section above it
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

    def remake_export_task(self, args):
        rmk = self._load(args)
        task = rmk.select_task(args.task_key, args.query)
        text = json.dumps(rmk.export_task(task), indent=1, default=repr)
        if args.output:
            Path(args.output).write_text(text + '\n')
        else:
            print(text)

    def remake_ls_tasks(self, args):
        from .core.dag import iter_expand_rule
        from .core.exceptions import Defer
        from .core.planner import make_predicate, rule_kwarg_names
        from .core.task import flat_items

        rmk = self._load(args)
        rmk.finalize()
        predicate = (make_predicate(args.query, rule_kwarg_names(rmk.rules))
                     if args.query else None)
        paint = Painter(args.colour)

        def input_files(task):
            for name, value in flat_items(task.inputs):
                info = {'name': name, 'path': str(value)}
                if args.check:
                    info['exists'] = Path(value).exists()
                yield info

        def output_files(task):
            for name, token in flat_items(task.outputs):
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
                            print(f'  - in  {f["name"]}: {f["path"]}{mark}')
                    if args.outputs:
                        for f in output_files(task):
                            mark = '' if not args.check else (
                                paint(' [complete]', 'green') if f['complete']
                                else paint(' [missing]', 'red', 'bold'))
                            print(f'  - out {f["name"]}: {f["path"]}{mark}')
            except Defer:
                logger.warning(f'{rule.name}: deferred (matrix not ready), tasks unknown')
        if args.json:
            _print_json({'tasks': rows})

    def remake_lint(self, args):
        # Not finalized: lint reads no metadata, and finalize would create the
        # DB in a fresh checkout (soak note 6).
        rmk = self._track(load_remake(args.remakefile, finalize=False, profile=args.profile))
        rows = rmk.lint()
        problems = [r for r in rows if r['kind'] != 'external']
        if args.json:
            _print_json({'findings': rows})
            return 1 if problems else 0

        from .util.pathdiff import display_path, plural, render_path_diff

        paint = Painter(args.colour)
        width = terminal_width()
        base = rmk.project_dir

        def path(p, label_width=13):
            # Relative to the project where possible, cut to the terminal.
            return display_path(p, base, max(width - label_width, 20) if width else None)

        def head(label, style, row, text):
            tasks = plural(row['count'], 'task')
            first = f"{label:<18}  {row['rule']} ({tasks}):"
            if width and len(first) + 1 + len(text) > width:
                # Too wide for one line: the explanation goes under it.
                print(f"{paint(f'{label:<18}', *style)}  {row['rule']} ({tasks}):")
                print(f'  - {text}')
            else:
                print(f"{paint(f'{label:<18}', *style)}  {row['rule']} ({tasks}): {text}")

        def show(label, p):
            print(f'  - {label + ":":<9}{path(p)}')

        # Problems first, by kind; the external lines (information) last.
        order = ('near_miss', 'missing_dependency', 'dir_nested', 'file_in_dir', 'external')
        ordered = sorted(rows, key=lambda r: order.index(r['kind'])
                         if r['kind'] in order else len(order))
        for n, row in enumerate(ordered):
            ex = row['example']
            if row['kind'] == 'external' and (n == 0 or ordered[n - 1]['kind'] != 'external') \
                    and n > 0:
                print()
            if row['kind'] == 'near_miss':
                head('NEAR MISS', ('yellow', 'bold'), row,
                     f"input produced by nothing; nearest is {row['other_rule']}'s output")
                for line in render_path_diff(display_path(ex['input'], base),
                                             display_path(ex['closest'], base), paint):
                    print(line)
            elif row['kind'] == 'missing_dependency':
                head('MISSING DEPENDENCY', ('red', 'bold'), row,
                     f"reads {row['other_rule']}'s output but does not depend_on it")
                show('input', ex)
            elif row['kind'] == 'dir_nested':
                head('DIR NESTED', ('red', 'bold'), row,
                     f"its Dir is inside {row['other_rule']}'s Dir")
                show('dir', ex['dir'])
                show('inside', ex['inside'])
            elif row['kind'] == 'file_in_dir':
                head('FILE IN DIR', ('red', 'bold'), row,
                     f"an output is inside {row['other_rule']}'s Dir")
                show('output', ex['file'])
                show('inside', ex['inside'])
            else:
                files = row.get('distinct', row['count'])
                count = plural(row['count'], 'input')
                if files != row['count']:
                    count += f" ({plural(files, 'file')})"
                first = f"{'external':<18}  {row['rule']}: {count} produced by no rule"
                if width and len(first) > width:
                    print(f"{paint(f'{"external":<18}', 'dim')}  {row['rule']}: {count}")
                    print('  - produced by no rule')
                else:
                    print(f"{paint(f'{"external":<18}', 'dim')}  {row['rule']}: {count} "
                          f"produced by no rule")
                show('e.g.', ex)
        unchecked = sorted({name for row in rows for name in row.get('unchecked', ())})
        if unchecked:
            print(paint(f"note: deferred rule(s) {', '.join(unchecked)} have outputs lint "
                        f"can't predict; near-miss and external findings above were not "
                        f"checked against them", 'dim'))
        if not rows:
            print('all inputs wired to declared dependencies')
        return 1 if problems else 0

    def remake_rule_dag(self, args):
        rmk = self._track(load_remake(args.remakefile, profile=args.profile))
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
            _print_json(data)
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

    def remake_dag(self, args):
        from .core.dot import rule_dot, task_dot

        rmk = self._track(load_remake(args.remakefile, profile=args.profile))
        text = task_dot(rmk) if args.tasks else rule_dot(rmk)
        if args.output:
            Path(args.output).write_text(text)
        else:
            print(text, end='')

    def remake_run_all(self, args):
        root = Path(args.directory).expanduser()
        if not root.is_dir():
            raise RemakeError(f'Not a directory: {root}')
        files = find_remakefiles(root, recursive=args.recursive)
        if not files:
            print(f'no remakefiles found in {root}')
            return 0
        flags = ['-E', args.executor]
        if args.nproc:
            flags += ['-j', str(args.nproc)]
        for on, flag in [(args.force, '-f'), (args.dry_run, '-n'),
                         (args.ignore_code_changes, '--ignore-code-changes'),
                         (args.check_outputs, '--check-outputs')]:
            if on:
                flags.append(flag)
        # Pass the top-level logging flags on to each invocation.
        top = [f for on, f in [(args.trace, '-T'), (args.debug, '-D'),
                               (args.warning, '-W')] if on]
        # The child's stderr is a pipe, so 'auto' would detect no colour:
        # decide it here, from our own terminal.
        colour = args.colour
        if colour == 'auto':
            from .util.colour import _auto_enabled
            colour = 'always' if _auto_enabled(sys.stdout) else 'never'
        top += ['--colour', colour]
        # Unbuffered, so task prints and log lines keep their order; safe
        # path (3.11+), so a remake.py in the cwd can't shadow the package.
        env = dict(os.environ, PYTHONUNBUFFERED='1', PYTHONSAFEPATH='1')
        results = []  # (file, status, detail)
        for f in files:
            print(f'=== {f}', flush=True)
            # Each remakefile in its own process: in one interpreter, a
            # second project's `import common` (or a same-named remakefile)
            # would resolve to the first one's module from sys.modules, and
            # run - and hash - the wrong code (code review 2026-11-21).
            # stdout goes straight through; stderr (logs, errors) is echoed
            # line by line so an import failure can be recognised.
            proc = subprocess.Popen(
                [sys.executable, '-m', 'remake', *top, 'run', str(f), *flags],
                stderr=subprocess.PIPE, text=True, env=env)
            import_error = None
            try:
                for line in proc.stderr:
                    sys.stderr.write(line)
                    sys.stderr.flush()
                    if import_error is None and 'could not import remakefile' in line:
                        import_error = line.strip().removeprefix('error: ')
                code = proc.wait()
            finally:
                # Ctrl-C / SIGTERM to run-all: stop the child, don't orphan it.
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
            if code == 2 and import_error:
                results.append((f, 'skipped', f'import failed: {import_error}'))
            elif code == EXIT_BLOCKED:
                results.append((f, 'blocked', 'rules left blocked'))
            else:
                results.append((f, 'ran' if not code else 'failed',
                                '' if not code else f'exit {code}'))
        print('\n=== run-all summary')
        for f, status, detail in results:
            print(f'{status:<8} {f}' + (f'  ({detail})' if detail else ''))
        counts = {k: sum(1 for r in results if r[1] == k)
                  for k in ('ran', 'failed', 'blocked', 'skipped')}
        print(f'{counts["ran"]} ran, {counts["failed"]} failed, '
              + (f'{counts["blocked"]} blocked, ' if counts['blocked'] else '')
              + f'{counts["skipped"]} skipped')
        # Same precedence as `run`: any failure is 1, else any blocked is 3.
        if counts['failed']:
            return EXIT_FAILED
        return EXIT_BLOCKED if counts['blocked'] else EXIT_OK

    def remake_rule_info(self, args):
        rmk = self._track(load_remake(args.remakefile, profile=args.profile))
        rule = rmk.rule_from_name(args.rule_name)
        data = rmk.rule_info(rule)
        if args.json:
            _print_json(data)
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
                print(f'  - {key}: {values!r}')

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
                print(f'  - {template}  ({name})')

        if data['uses']:
            print('uses:')
            for entry in data['uses']:
                if entry['kind'] == 'value':
                    print(f'  - {entry["name"]} = {entry["rendering"]}')
                elif entry['kind'] == 'source':
                    print(f'  - {entry["name"]}:')
                    for line in entry['rendering'].rstrip().splitlines():
                        print(f'      {line}')  # source code: no dashes
                else:
                    print(f'  - {entry["name"]}: '
                          + paint(f'{entry["rendering"]} (source unavailable)', 'dim'))
        if data['config']:
            print(f'config:      {data["config"]!r}')

    def remake_task_info(self, args):
        rmk = self._load(args)
        task = rmk.select_task(args.task_key, args.query)
        data = rmk.task_info(task)
        if args.json:
            _print_json(data)
            return

        from .util.pathdiff import display_path

        paint = Painter(args.colour)
        width = terminal_width()

        def fit(path, reserve=0):
            # Relative to the project, cut to the terminal (10 = the label).
            return display_path(path, rmk.project_dir,
                                max(width - 10 - reserve, 20) if width else None)

        print(f'{task}  {task.key}')
        when = f' at {data["timestamp"]}' if data['timestamp'] else ''
        print(f'status:   {paint.status(data["status"])}{when}')
        for name, path_info in data['inputs'].items():
            mark = (paint('exists', 'green') if path_info['exists']
                    else paint('missing', 'red', 'bold'))
            print(f'input:    {fit(path_info["path"], 14 + len(name))}  [{mark}]  ({name})')
        for name, path_info in data['outputs'].items():
            mark = (paint('complete', 'green') if path_info['complete']
                    else paint('missing', 'red', 'bold'))
            print(f'output:   {fit(path_info["path"], 16 + len(name))}  [{mark}]  ({name})')
            dir_info = path_info.get('dir')
            if dir_info and dir_info['n_stale']:
                print(paint(f'  - {dir_info["n_stale"]} stale file(s), '
                            f'{_format_bytes(dir_info["stale_bytes"])}, not produced by '
                            f'the last run (see the manifest; remake never deletes them)',
                            'yellow'))
        stats = data.get('output_stats') or {}
        if stats:
            # Plain files are summed under '*'; a Dir (or a checksummed file)
            # has its own entry; other outputs (ZarrStore, S3Object, a plain
            # path that turned out to be a directory) record no size: say so,
            # or one file's size reads as the task's.
            from .core.task import flat_items
            from .core.tokens import FileToken
            from .util.pathdiff import plural

            def size(st):
                text = (f'{plural(st["n_files"] or 0, "file")}, '
                        f'{_format_bytes(st["total_bytes"] or 0)}')
                return text + (f', sha256 {st["checksum"]}' if st.get('checksum') else '')

            entries = [(name, size(st)) for name, st in stats.items() if name != '*']
            unmeasured = [
                (name, f'not measured ({type(token).__name__})')
                for name, token in flat_items(task.outputs)
                if name not in stats
                and not ('*' in stats and isinstance(token, FileToken)
                         and not os.path.isdir(token.path))]
            if '*' in stats:
                entries.append(('other files' if entries or unmeasured else None,
                                size(stats['*'])))
            entries += unmeasured
            if len(entries) == 1 and entries[0][0] is None:
                print(f'size:     {entries[0][1]}')
            else:
                print('size:')
                for label, text in entries:
                    print(f'  - {label}: {text}')
        last = data.get('last_execution')
        if last:
            where = f' on {last["host"]}' if last['host'] else ''
            why = f' ({last["reason"]})' if last['reason'] else ''
            print(f'last ran: run {last["run_seq"]} at {last["finished"]}{where}{why}')
        resources = _resources_line(data['resources'])
        if resources:
            # Of the last execution — set-state doesn't un-measure a run.
            print(f'ran with: {resources}')
        if last:
            run = last.get('run') or {}
            if run.get('git_commit'):
                dirty = (f' + uncommitted changes (diff {run["git_diff_hash"][:12]})'
                         if run.get('git_dirty') and run.get('git_diff_hash') else
                         ' + uncommitted changes' if run.get('git_dirty') else '')
                print(f'git:      {run["git_commit"][:12]}{dirty}')
            if run.get('remake_version'):
                print(f'env:      Python {run["python_version"]}, remake {run["remake_version"]}'
                      + (f', {run["lockfile"]} {run["lockfile_hash"][:12]}'
                         if run.get('lockfile') and run.get('lockfile_hash') else ''))
            if last['different_environment']:
                print(paint('  - ran in a different environment from the one that '
                            'planned it (see `remake history` and `remake check-env`)',
                            'yellow'))
        log = data['log']
        # The local executor runs tasks in-process and writes no task log.
        mark = '' if log['exists'] else paint('  [no log file]', 'dim')
        print(f'log:      {fit(log["path"], 0 if log["exists"] else 15)}{mark}')
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

    def remake_history(self, args):
        """`remake history`: one task's executions, recent runs, or a prune."""
        from .metadata.metadata_manager import STATUS_NAMES

        if args.keep is not None and not args.prune:
            raise RemakeError('--keep only applies with --prune')
        rmk = self._load(args)
        meta = rmk.metadata
        # SLURM jobs leave their results as sidecars: absorb them first, or
        # history lags the jobs that have finished (JASMIN check 2026-11-22).
        meta.ingest_sidecars(rmk.rules)
        if args.prune:
            keep = args.keep if args.keep is not None else (
                (rmk.config.get('history') or {}).get('keep'))
            deleted = meta.prune_history(keep=keep)
            logs = rmk.prune_task_logs()
            if args.json:
                _print_json({'deleted': deleted, 'logs_deleted': logs})
            else:
                print(f'pruned {deleted} history row(s), {logs} stale task log(s)')
            return 0
        if args.runs:
            runs = meta.get_runs(limit=args.limit)
            if args.json:
                _print_json({'runs': runs})
                return 0
            table = []
            for r in runs:
                counts = ''
                # A SLURM submission (no waves: never run locally), so far;
                # an interrupted local run has n_waves and keeps its counts.
                if r.get('n_run') is not None and r.get('n_planned') is not None \
                        and r.get('finished') is None and r.get('n_waves') is None:
                    counts = (f'{r["n_planned"]} submitted, {r["n_run"]} run, '
                              f'{r["n_failed"] or 0} failed')
                elif r.get('n_run') is not None:
                    counts = (f'{r["n_run"]} run, {r["n_failed"] or 0} failed'
                              + (f', {r["n_blocked_rules"]} blocked '
                                 f'{"rule" if r["n_blocked_rules"] == 1 else "rules"}'
                                 if r.get('n_blocked_rules') else ''))
                elif r.get('n_planned') is not None:
                    counts = f'{r["n_planned"]} submitted'
                git = ''
                if r.get('git_commit'):
                    git = r['git_commit'][:10] + ('+dirty' if r.get('git_dirty') else '')
                parent = f'continues {r["parent_run_seq"]}' if r.get('parent_run_seq') else ''
                table.append((r['run_seq'], r['started'], r['command'] or '', counts, git,
                              parent))
            if not table:
                print('no recorded runs (history starts with remake 0.10)')
                return 0
            for line in render_table(('run', 'started', 'command', 'tasks', 'git', 'note'),
                                     table, width=terminal_width()):
                print(line)
            return 0
        task = rmk.select_task(args.task_key, args.query)
        rows = meta.get_history(task, limit=args.limit)
        for row in rows:
            row['status'] = STATUS_NAMES.get(row['status'], 'pending')
            row['different_environment'] = (row['env_id'] is not None
                                            and row['env_id'] != row['run_env_id'])
        if args.json:
            _print_json({'task': str(task), 'key': task.key, 'executions': rows})
            return 0
        paint = Painter(args.colour)
        print(f'{task}  {task.key}')
        if not rows:
            print('no recorded executions (history starts with remake 0.10)')
            return 0
        table, notes = [], []
        for row in rows:
            table.append((row['run_seq'], row['finished'], row['status'],
                          _resources_line(row) or '', row['reason'] or '', row['host'] or ''))
            notes.append([(paint(f'  - {row["error"]}', 'red') if row['error'] else None),
                          (paint('  - ran in a different environment from the planning run',
                                 'yellow') if row['different_environment'] else None)])
        status_style = [None, None, lambda v: STATUS_STYLE.get(v), None, None, None]
        lines = render_table(('run', 'finished', 'status', 'resources', 'reason', 'host'),
                             table, paint=paint, styles=status_style, width=terminal_width())
        print(lines[0])
        for line, extra in zip(lines[1:], notes):
            print(line)
            for note in extra:
                if note:
                    print(note)
        return 0

    def remake_why(self, args):
        rmk = self._load(args)
        results = list(rmk.why(args.task_key, args.query))
        if args.json:
            _print_json({'tasks': [
                {'task': str(task), 'key': task.key, 'will_run': bool(will_run),
                 'reasons': [{'category': r.category, 'message': r.message}
                             for r in reasons]}
                for task, will_run, reasons in results]})
            return
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
            print(f'- will run: {verdict}')
            if not reasons:
                print(paint('- up to date: recorded success, code and uses '
                            'unchanged, no upstream reruns', 'dim'))
            for reason in reasons:
                # The reasons belong to "will run: yes": nested under it.
                print(paint(f'  - {reason.message}', 'yellow'))
            if len(results) > 1:
                print()  # between tasks, and before the summary
        if len(results) > 1:
            print(f'{len(results)} task(s): {n_run} would run, '
                  f'{len(results) - n_run} up to date')

    def remake_config(self, args):
        import json

        rmk = self._track(load_remake(args.remakefile, finalize=False, profile=args.profile))
        eff = rmk.effective_config
        if args.json:
            _print_json(eff.to_json())
            return 0
        print(f'profile: {eff.profile or "(none)"}'
              + (f' (from {eff.profile_source})' if eff.profile else ''))
        print('files consulted:')
        for layer, path, exists in eff.files:
            print(f'  - {layer}: {path} ({"found" if exists else "not found"})')
        print('config (layer that set each key):')

        def walk(d, prefix=''):
            for k in sorted(d):
                v = d[k]
                if isinstance(v, dict):
                    yield from walk(v, f'{prefix}{k}.')
                else:
                    yield f'{prefix}{k}', v

        rows = list(walk(eff.values))
        for key, v in rows:
            print(f'  - {key} = {v!r}  [{eff.provenance.get(key, "?")}]')
        if not rows:
            print('  (empty)')
        return 0

    def remake_slurm_status(self, args):
        import datetime as dt

        from .core.dag import expand_rule
        from .executors.slurm_executor import (_read_sidecar, load_job_specs,
                                               recorded_jobids, spec_path,
                                               squeue_snapshot)
        from .metadata.metadata_manager import STATUS_NAMES
        from .util.paths import jobs_dir

        rmk = self._load(args)
        snapshot = squeue_snapshot()
        # Finished elements leave their results as sidecars: absorb them, or
        # the done/failed counts lag the queue.
        rmk.metadata.ingest_sidecars(rmk.rules)
        rows = []
        for rule in rmk.rules:
            sidecar_path = jobs_dir() / f'{rule.name}.jobids.json'
            recorded = _read_sidecar(sidecar_path)
            if recorded is None:
                continue
            jobids = recorded_jobids(rule.name)
            submitted = dt.datetime.fromtimestamp(
                sidecar_path.stat().st_mtime).isoformat(sep=' ', timespec='seconds')
            # The submission's tasks (its job spec), and how many of them the
            # DB records as done/failed BY this submission (a record from an
            # earlier run has a lower run_seq: a resubmitted task that
            # succeeded before is not done yet). None when unknown: no spec
            # (pruned, or a pre-0.9 sidecar without run_seq) or no matrix.
            run_seq = recorded.get('run_seq')
            ntasks = succeeded = failed = None
            specs = spec_path(rule.name, run_seq)
            if run_seq is not None and specs.exists():
                keys = {spec['task_key'] for spec in load_job_specs(specs)}
                ntasks = len(keys)
                try:
                    tasks = [t for t in expand_rule(rule) if t.key in keys]
                except Exception:  # Defer, or a callable matrix that raises
                    tasks = None
                if tasks is not None:
                    records = rmk.metadata.get_tasks_status(tasks) if tasks else {}
                    db = Counter(STATUS_NAMES.get(rec.status, 'pending')
                                 for rec in records.values()
                                 if rec.run_seq is not None and rec.run_seq >= run_seq)
                    succeeded, failed = db['success'], db['failed']
            # One row per rule: a pre-arrays sidecar lists one job id per task.
            elements = [e for jobid in jobids for e in snapshot.get(jobid, [])]
            states = Counter(state for _, state, _ in elements)
            reasons = sorted(
                {reason for *_, reason in elements if reason and reason != 'None'}
            )
            rows.append({
                'rule': rule.name,
                'jobid': ','.join(jobids),
                'states': dict(states),
                'reasons': reasons,
                'submitted': submitted,
                'tasks': ntasks,
                'succeeded': succeeded,
                'failed': failed,
            })
        if args.json:
            _print_json({'submissions': rows})
            return
        if not rows:
            print('No SLURM submissions recorded (.remake/jobs/*.jobids.json)')
            return
        paint = Painter(args.colour)
        header = ('rule', 'job', 'submitted', 'tasks', 'PD', 'R', 'done', 'failed', 'reason')
        table, totals = [], Counter()
        for row in rows:
            states = Counter(row['states'])
            pd, r = states.pop('PD', 0), states.pop('R', 0)
            note = []
            if not row['states']:
                note.append('not queued')
            note += [f'{k}:{v}' for k, v in sorted(states.items())]  # CG, S, ...
            note += row['reasons']
            table.append((row['rule'], row['jobid'], row['submitted'][:16], row['tasks'],
                          pd, r, row['succeeded'], row['failed'], ', '.join(note)))
            for k, v in (('tasks', row['tasks']), ('PD', pd), ('R', r),
                         ('done', row['succeeded']), ('failed', row['failed'])):
                totals[k] += v or 0
        total = ('TOTAL', '', '', totals['tasks'], totals['PD'], totals['R'],
                 totals['done'], totals['failed'], '')

        def style(st):
            return lambda v: st if _is_count(v) and v != 0 else None

        styles = [None, None, None, None, style(('yellow',)), style(('cyan',)),
                  style(('green',)), style(('red', 'bold')), None]
        for line in render_table(header, table, total=total, paint=paint, styles=styles,
                                 width=terminal_width(), shrink=8):
            print(line)
        print(paint('PD pending, R running (squeue); done/failed: DB, this submission',
                    'dim'))

    def remake_check_env(self, args):
        from . import envcheck

        rmk = self._load(args)
        local = envcheck.environment_report()
        job, error = envcheck.job_report(
            args.remakefile, args.srun, rmk.config.get('slurm', {})
        )
        mismatches = envcheck.compare_reports(local, job, error)
        if args.json:
            _print_json({'planning': local, 'job': job,
                              'mismatches': mismatches})
        else:
            print(envcheck.format_text(local, job, mismatches))
        return 1 if mismatches else 0

    def remake__env_report(self, args):
        from . import envcheck

        # finalize=False: only the imports matter here, and under --srun this
        # runs on a compute node, which must not open the shared DB (as
        # run-array-task doesn't).
        load_remake(args.remakefile, finalize=False, profile=args.profile)
        print(json.dumps(envcheck.environment_report()))

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
    # Honour --colour / NO_COLOR / a non-TTY stderr like the rest of the
    # output (review 2026-09-24 L25: hardcoded colorize=True filled CI logs
    # and SLURM .err files with escape codes).
    colorize = Painter(getattr(args, 'colour', 'auto'), sys.stderr).enabled
    if args.trace:
        logger.add(sys.stderr, colorize=colorize, level='TRACE')
    elif args.debug:
        logger.add(sys.stderr, colorize=colorize, level='DEBUG')
    elif args.warning:
        logger.add(
            sys.stderr, colorize=colorize, format='<lvl>{message}</lvl>', level='WARNING'
        )
    else:
        logger.add(
            sys.stderr, colorize=colorize, format='<lvl>{message}</lvl>', level='INFO'
        )

    # Anchor execution to the remakefile's directory: cd there so .remake/ and
    # the pipeline's relative paths resolve next to the remakefile, not wherever
    # the command happened to be invoked (todos.md). The embedded remakefile arg
    # becomes a bare name, so the SLURM scripts (which re-invoke `remake
    # run-array-task <remakefile>` from the submit dir) stay consistent.
    # Restored after dispatch so in-process/library callers don't see cwd move.
    orig_cwd = os.getcwd()
    # An output *file* (dag, export-task) is relative to where remake was
    # invoked, not the remakefile's directory it chdirs into below. (Some
    # commands' `output` is a show-outputs flag, not a path.)
    if isinstance(getattr(args, 'output', None), str):
        args.output = os.path.abspath(args.output)
    if getattr(args, 'remakefile', None):
        rf = Path(args.remakefile).expanduser()
        if str(rf.parent) not in ('', '.'):
            try:
                os.chdir(rf.parent)
            except OSError as e:
                print(f'error: cannot enter remakefile directory {rf.parent}: {e}',
                      file=sys.stderr)
                return 2
        args.remakefile = rf.name

    # Per-task-process subcommands (SLURM array elements) get a per-task log
    # sink instead — concurrent appends to the shared logs corrupt them on
    # NFS-class filesystems (see design_docs/designs/per_task_logging.md).
    per_task = args.subcmd_name in ('run-task', 'run-array-task')
    if hasattr(args, 'remakefile') and not per_task:
        import uuid

        # Every record from this invocation shares one run_id (surfaced in the
        # structured sink), so a miner can group an invocation's lines and
        # correlate e.g. a plan total with its constituent status queries.
        logger.configure(extra={'run_id': uuid.uuid4().hex[:12]})
        try:
            # metadata_dir from remake.toml, before the config is loaded
            # (the log sinks below need it).
            set_metadata_dir(project_metadata_dir(Path.cwd()), base=Path.cwd())
            metadata_dir().mkdir(parents=True, exist_ok=True)
        except (OSError, RemakeError) as e:
            print(f'error: cannot create metadata dir {metadata_dir()}: {e}', file=sys.stderr)
            os.chdir(orig_cwd)
            return 2
        debug_level = 'TRACE' if args.trace else 'DEBUG'
        # Three always-on file sinks next to the metadata DB, split so the
        # streams don't compete for one rotation window (logs_analysis §3.2):
        #  - remake.log: the human-facing run narrative (INFO+).
        #  - remake.debug.log: the DEBUG (TRACE under -T) firehose.
        #  - remake.jsonl: structured mirror of the firehose — one JSON object
        #    per record, with bound fields (event=..., ntasks=..., seconds=...)
        #    under record.extra, so mining is jq, not regex (logs_analysis §4).
        logger.add(
            log_file('remake.log'), level='INFO',
            rotation='5 MB', retention=3,
        )
        logger.add(
            log_file('remake.debug.log'), level=debug_level,
            rotation='5 MB', retention=3,
        )
        logger.add(
            log_file('remake.jsonl'), level=debug_level, serialize=True,
            rotation='5 MB', retention=3,
        )
        logger.bind(event='invocation').debug(f'argv: {argv}')

    if getattr(args, 'debug_exception', False):
        # Handle top level exceptions with a debugger (run -X only).
        sys.excepthook = exception_info

    # SIGTERM (kill, scancel, a CI timeout) stops `run` the same way Ctrl-C
    # does, so the executors' cleanup runs: queued tasks cancelled, workers
    # terminated, the run lock released (review 2026-09-24 M10). By default
    # SIGTERM killed the parent outright and left its workers running.
    received = {}
    old_sigterm = None
    old_profile_env = os.environ.get('REMAKE_PROFILE')

    def _on_sigterm(signum, frame):
        received['signal'] = signum
        raise KeyboardInterrupt

    try:
        # Only the main thread may install signal handlers: an in-process
        # caller running `run` from another thread keeps the default SIGTERM
        # behaviour rather than failing. Inside the try so a failure here
        # can never skip the cwd restore below.
        if (args.subcmd_name in ('run', 'run-all')
                and threading.current_thread() is threading.main_thread()):
            old_sigterm = signal.signal(signal.SIGTERM, _on_sigterm)
        if args.profile:
            # Also seen by the SLURM jobs this process submits (sbatch
            # exports the environment), so continuation jobs use it too.
            os.environ['REMAKE_PROFILE'] = args.profile
        return cli.dispatch()
    except KeyboardInterrupt:
        signum = received.get('signal', signal.SIGINT)
        if getattr(args, 'debug_exception', False):
            raise
        print(f'interrupted ({signal.Signals(signum).name})', file=sys.stderr)
        return 128 + signum  # the shell convention: 130 Ctrl-C, 143 SIGTERM
    except BrokenPipeError:
        # Output piped into something that stopped reading (`| head`): stop
        # quietly like other CLI tools, not with a traceback (review L26).
        # Point stdout at devnull so the interpreter's final flush can't
        # raise again.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        return 141  # 128 + SIGPIPE, what a shell reports for a killed writer
    except RemakeError as e:
        # User-facing errors (bad query, >1-task match, unknown rule, ...)
        # print cleanly and exit 2; keep the traceback only under -X.
        if getattr(args, 'debug_exception', False):
            raise
        print(f'error: {e}', file=sys.stderr)
        return 2
    finally:
        if args.profile:
            if old_profile_env is None:
                os.environ.pop('REMAKE_PROFILE', None)
            else:
                os.environ['REMAKE_PROFILE'] = old_profile_env
        if old_sigterm is not None:
            signal.signal(signal.SIGTERM, old_sigterm)
        for rmk in cli.loaded:
            close = getattr(rmk.metadata, 'close', None)
            if close is not None:
                close()
        os.chdir(orig_cwd)


if __name__ == '__main__':
    remake_cmd()
