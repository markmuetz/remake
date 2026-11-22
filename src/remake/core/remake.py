"""The Remake class — wires rules, planner, metadata and executors together."""
import inspect
import os
import re
import traceback
from collections import Counter
from pathlib import Path
from time import perf_counter

import networkx as nx
from loguru import logger

from ..metadata.metadata_manager import (
    STATUS_NAMES,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_SUCCESS,
    RecordCache,
)
from ..util import task_log_path
from ..util.resources import capture_for_config
from ..util.run_lock import run_lock
from .dag import build_rule_dag, expand_rule, iter_expand_rule
from .deps import Edges
from .exceptions import Defer, OutputValidationError, RemakeError, TaskExit
from .planner import (
    cascade_settled,
    explain_task,
    make_predicate,
    plan,
    rule_kwarg_names,
    upstream_run_seqs,
)
from .rule import Rule
from .scope import check_scope, exec_function
from .task import Task
from .tokens import Dir, OutputToken, PathToken


class _TemplatePlaceholder:
    """Stands in for a matrix kwarg while deriving path templates
    (`Remake._part_templates`). Renders as '{name}' via str()/f-string
    interpolation and nothing else: it is deliberately not a str and defines
    no arithmetic/comparison, so a callable that *computes* with the value
    (rather than formatting it into a path) raises and the template is
    reported as not derivable — instead of silently wrong. A format spec
    ('{n:03d}') also raises: it would render differently for real values."""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return '{' + self.name + '}'

    def __format__(self, spec):
        if spec:
            raise ValueError(
                f'format spec {spec!r} on {{{self.name}}} renders '
                f'value-dependently')
        return str(self)


def _resource_fields(resources):
    """Measured resources as JSONL log fields, omitting what was not
    measured (`max_rss_bytes` is None on a task-reusing process without
    /proc — see util/resources.py)."""
    fields = {}
    if resources['cpu_s'] is not None:
        fields['cpu_s'] = round(resources['cpu_s'], 6)
    if resources['max_rss_bytes'] is not None:
        fields['max_rss_bytes'] = resources['max_rss_bytes']
        fields['rss_method'] = resources['rss_method']
    return fields


def _blocked_reason(rule):
    """Why a rule is still deferred when a local run has nothing left to
    try: its matrix's Defer paths if it still can't expand, else (it would
    expand, so the planner is holding it back) an upstream that didn't
    complete this run."""
    from .rule import is_deferrable

    if is_deferrable(rule.matrix):
        try:
            rule.matrix()
        except Defer as e:
            waiting = ', '.join(e.paths) if e.paths else 'an unspecified input'
            return f'matrix not ready — waiting on {waiting}'
        except Exception as e:  # the matrix itself is broken: say so
            return f'matrix raised {type(e).__name__}: {e}'
    return 'an upstream rule did not complete (see failures above)'


def _where(rule):
    fn = rule.fn
    return f'{getattr(fn, "__module__", "?")}.{getattr(fn, "__qualname__", rule.name)}'


def _same_definition(a, b):
    """Two Rule objects for the same function re-defined (e.g. a notebook
    cell re-run): same module and qualified name, but freshly compiled code.
    Distinct functions that merely share a rule name (two modules each
    defining `process`, a clashing name=) are not — nor are rules built by
    one factory function, which share a single code object and differ only
    in closure values."""
    return _where(a) == _where(b) and a.fn.__code__ is not b.fn.__code__


def _config_edits(before, after):
    """The parts of `after` that differ from `before` (nested dicts walked;
    deletions are not tracked)."""
    patch = {}
    for key, value in after.items():
        old = before.get(key, _MISSING)
        if isinstance(value, dict) and isinstance(old, dict):
            sub = _config_edits(old, value)
            if sub:
                patch[key] = sub
        elif old is _MISSING or old != value:
            patch[key] = value
    return patch


def _deep_update(base, patch):
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value


_MISSING = object()


# A scheme prefix (s3://, https://, gs://): an input that is a URL.
# Path('s3://b/k') stringifies as 's3:/b/k', hence :// or :/ (but not a
# Windows drive, C:\\).
_URL_RE = re.compile(r'^[A-Za-z][A-Za-z0-9+.-]+:/')


def _accepts_output_stats(metadata):
    try:
        return 'output_stats' in inspect.signature(metadata.update_task).parameters
    except (TypeError, ValueError):
        return False


class Remake:
    def __init__(
        self,
        *,
        rules=None,
        config=None,
        metadata=None,
        check_outputs='never',
        strict_scope=False,
    ):
        # What the caller passed; the effective config (defaults < profile <
        # user file < project file < this) is built lazily — see `config`.
        self._programmatic_config = dict(config) if config else {}
        if config:
            from ..util.config import validate_programmatic

            validate_programmatic(self._programmatic_config)
        self._effective = None
        self._effective_snapshot = None
        self.metadata = metadata
        self.check_outputs = check_outputs
        # Rules the last run() left unresolved (deferred matrix never ready,
        # or waiting on an upstream that failed). Non-empty means that run
        # did not complete everything; the CLI exits non-zero on it.
        self.blocked_rules = []
        self.strict_scope = strict_scope
        self.rules = []
        self.dag = None
        self.remakefile = None  # set by load_remake
        self._finalized = False
        if rules:
            self.add_rules(rules)

    # --- configuration ---

    def resolve_config(self, project_dir=None, profile=None):
        """(Re)build the effective config (util/config.py): `remake.toml` is
        looked for in `project_dir` (default cwd); `profile` is the CLI
        selection. The CLI calls this once at load time."""
        import copy

        from ..util.config import resolve

        if self._effective is not None:
            # The remakefile may have changed `rmk.config` in place (e.g.
            # `rmk.config['slurm']['account'] = ...`) before load_remake
            # re-resolves it: keep those edits as programmatic settings
            # (ultrareview 2026-11-21).
            patch = _config_edits(self._effective_snapshot, self._effective.values)
            if patch:
                _deep_update(self._programmatic_config, patch)
        self._effective = resolve(
            project_dir, programmatic=self._programmatic_config, profile=profile)
        self._effective_snapshot = copy.deepcopy(self._effective.values)
        return self._effective

    @property
    def effective_config(self):
        """The `EffectiveConfig` (values + per-key provenance), built on first use."""
        if self._effective is None:
            self.resolve_config()
        return self._effective

    @property
    def config(self):
        """The merged config as a plain dict (what executors read)."""
        return self.effective_config.values

    @config.setter
    def config(self, value):
        # Compatibility: replace the programmatic layer and re-merge.
        self._programmatic_config = dict(value or {})
        self._effective = None

    def _record_run(self, command):
        self.metadata.record_run(
            remakefile=self.remakefile, command=command, config=self.config)

    # --- registration ---

    def add_rules(self, rules):
        for rule in rules:
            if not isinstance(rule, Rule):
                raise RemakeError(f'Not a Rule (use the @rule decorator): {rule!r}')
            if rule in self.rules:
                continue
            # Task keys are sha1('<rule name>:<kwargs>'), so two rules sharing
            # a name share task keys and DB records (review 2026-09-24 H8).
            existing = next((r for r in self.rules if r.name == rule.name), None)
            if existing is not None and not _same_definition(existing, rule):
                raise RemakeError(
                    f'Duplicate rule name {rule.name!r}: defined by '
                    f'{_where(existing)} and {_where(rule)}. Rule names must be '
                    f'unique within a pipeline (rename one, or pass name=...)'
                )
            rule.remake = self
            if existing is not None:
                # The same function re-defined — a notebook cell or script
                # section executed again: the new definition replaces the old.
                logger.warning(f'Rule {rule.name!r} redefined: replacing the earlier definition')
                self.rules[self.rules.index(existing)] = rule
            else:
                self.rules.append(rule)
        self._finalized = False

    def rules_from_current_module(self):
        """Register the Rules bound to names in the calling module and
        defined in it (the rule function's `__module__` is this module).
        Rules imported from elsewhere are not claimed, and neither are Rules
        held inside lists, tuples or dicts; both are named in one warning
        advising `add_rules`. Call this after any `add_rules`, so rules
        registered explicitly are not reported."""
        # f_locals so rules defined inside functions (tests, notebooks) are
        # found too; at module level locals and globals are the same dict.
        frame = inspect.currentframe().f_back
        namespace = {**frame.f_globals, **frame.f_locals}
        self._scan_namespace(namespace, namespace.get('__name__'), 'this module')

    def rules_from_modules(self, *modules):
        """Register the Rules bound to top-level names in each module and
        defined in that module. Rules a module merely imported, or holds in
        containers, are not claimed and are named in a warning."""
        for module in modules:
            self._scan_namespace(vars(module), module.__name__, module.__name__)

    def _scan_namespace(self, namespace, module_name, label):
        """Add the Rules in `namespace` defined in `module_name`; warn once
        about Rules seen but not registered (imported, or in a container)."""
        own, seen = [], []  # seen: (description, rule) not claimed
        for name, value in namespace.items():
            if name.startswith('__'):
                continue
            if isinstance(value, Rule):
                if getattr(value.fn, '__module__', None) == module_name:
                    own.append(value)
                else:
                    seen.append((f'{name} (imported from {_where(value)})', value))
            elif isinstance(value, (list, tuple)):
                seen.extend((f'{name}[{i}]', v) for i, v in enumerate(value)
                            if isinstance(v, Rule))
            elif isinstance(value, dict):
                seen.extend((f'{name}[{k!r}]', v) for k, v in value.items()
                            if isinstance(v, Rule))
        self.add_rules(own)
        # Skip any that are registered anyway (also bound to a claimed name,
        # or added earlier with add_rules).
        unregistered = [d for d, r in seen if not any(r is x for x in self.rules)]
        if unregistered:
            logger.warning(
                f'Scan of {label} did not register: {", ".join(unregistered)}. '
                f'Register rules imported from other modules, or held in '
                f'lists/dicts, explicitly with rmk.add_rules(...)'
            )

    # --- planning ---

    def finalize(self):
        if self.metadata is None:
            from ..metadata.sqlite3_backend import Sqlite3Backend

            self.metadata = Sqlite3Backend()
        self._check_scopes()
        self.dag = build_rule_dag(self.rules)
        self.metadata.ensure_rules(self.rules, remakefile=self.remakefile)
        self._finalized = True
        return self

    def _check_scopes(self):
        """Scope-check every rule once, now that the remakefile has fully
        executed (see rule()); strictness is the rule's own, else the
        Remake default."""
        for rule in self.rules:
            if getattr(rule, '_scope_checked', False):
                continue
            strict = rule.strict_scope if rule.strict_scope is not None else self.strict_scope
            check_scope(rule.fn, rule.uses, strict=bool(strict))
            rule._scope_checked = True

    def plan(self, query=None, force=False, ignore_code_changes=False, restamp=None):
        if not self._finalized:
            self.finalize()
        # Results recorded by SLURM array elements live in sidecar files
        # (they can't write the DB concurrently); fold them in before the
        # DB is read for planning.
        self.metadata.ingest_sidecars(self.rules)
        return plan(
            self.rules,
            self.dag,
            self.metadata,
            query=query,
            force=force,
            check_outputs=self.check_outputs,
            ignore_code_changes=ignore_code_changes,
            restamp=restamp,
        )

    def explain_task(self, task):
        """(will_run, reasons) for one task — `remake why`."""
        if not self._finalized:
            self.finalize()
        self.metadata.ingest_sidecars(self.rules)
        # Read-only: the internal plan() and the explanation ask for
        # overlapping records — fetch each from the DB at most once.
        cache = RecordCache(self.metadata)
        return explain_task(
            self.rules, self.dag, cache, task, check_outputs=self.check_outputs
        )

    def explain_tasks(self, tasks=None):
        """Yield (task, will_run, reasons) for each task — batch `remake why`.
        Plans once and reuses the runnable set, so cost is one plan() plus
        per-task record/stat checks, not one plan() per task. `tasks=None`
        explains the runnable set itself (bare `remake why`)."""
        if not self._finalized:
            self.finalize()
        self.metadata.ingest_sidecars(self.rules)
        # Read-only: one record cache shared by the plan and every per-task
        # explanation. Without it, the durable-propagation check re-queried
        # each upstream rule's full record set once per explained task —
        # N tasks × M upstream records, the worst redundancy found in the
        # bug 04 audit. With it, plan() warms the cache and the per-task
        # checks are dict hits.
        cache = RecordCache(self.metadata)
        runnable, _ = plan(
            self.rules, self.dag, cache, check_outputs=self.check_outputs
        )
        # One edge map and run_seq view for the batch (the same N × M trap).
        edges, seqs = Edges(), upstream_run_seqs(cache)
        for task in runnable if tasks is None else tasks:
            will_run, reasons = explain_task(
                self.rules, self.dag, cache, task,
                check_outputs=self.check_outputs, runnable=runnable,
                edges=edges, upstream_seqs=seqs,
            )
            yield task, will_run, reasons

    def why(self, key=None, query=None):
        """Explain why task(s) would (or would not) rerun — the data behind
        `remake why`, with CLI-style selection: one task by key, all matches
        for a query, or the runnable set when neither is given. Yields
        (task, will_run, reasons) via explain_tasks (one plan() shared)."""
        if key and query:
            raise RemakeError('Give a task key or a -Q query, not both')
        if key:
            tasks = [self.task_from_key(key)]
        elif query:
            tasks = self.tasks(query=query)
            if not tasks:
                raise RemakeError(f'No task matches {query!r}')
        else:
            tasks = None  # default: explain the runnable set
        return self.explain_tasks(tasks)

    def iter_tasks(self, query=None):
        """Lazily yield tasks of all currently-expandable rules, one at a
        time — constant memory regardless of matrix size.

        Rules whose @deferrable matrix raises Defer (an upstream output they
        need does not exist yet) are skipped with a warning rather than
        crashing — their task set is unknowable until their upstreams run. This
        keeps introspection commands (why, task-info, set-state) usable while a
        dynamic matrix is deferred."""
        if not self._finalized:
            self.finalize()
        predicate = make_predicate(query, rule_kwarg_names(self.rules)) if query else None
        for rule in self.rules:
            try:
                yield from iter_expand_rule(rule, predicate)
            except Defer:
                logger.warning(
                    '{}: deferred (matrix not ready), tasks unknown', rule.name
                )

    def tasks(self, query=None):
        """All tasks of all currently-expandable rules. This materialises
        every task — intended for info/reporting, not planning."""
        return list(self.iter_tasks(query))

    def task_from_spec(self, rule_name, kwargs):
        """Construct a task directly from (rule name, kwargs) — no search.
        This is the executor-facing lookup (e.g. SLURM job specs carry
        rule + kwargs)."""
        for rule in self.rules:
            if rule.name == rule_name:
                return Task(rule=rule, kwargs=dict(kwargs))
        raise RemakeError(f'No rule named {rule_name}')

    def task_from_key(self, key):
        """Find a task by key or unambiguous key prefix.

        Streams lazily; a full-length key returns on first match. A prefix
        must scan all tasks to detect ambiguity (hashes are not invertible),
        but never materialises more than the matches.
        """
        full_length = len(key) == 40
        matches = []
        for task in self.iter_tasks():
            if task.key.startswith(key):
                if full_length:
                    return task
                matches.append(task)
                if len(matches) > 1:
                    raise RemakeError(f'Task key prefix {key} is ambiguous')
        if not matches:
            raise RemakeError(f'No task with key {key}')
        return matches[0]

    def select_task(self, key=None, query=None):
        """Resolve the single task a command addresses: a key (or unambiguous
        prefix), or a query matching exactly one task. Raises if both/neither
        are given, or a query is not uniquely satisfied."""
        if key and query:
            raise RemakeError('Give a task key or a -Q query, not both')
        if key:
            return self.task_from_key(key)
        if query:
            tasks = self.tasks(query=query)
            if not tasks:
                raise RemakeError(f'No task matches {query!r}')
            if len(tasks) > 1:
                raise RemakeError(
                    f'{len(tasks)} tasks match {query!r}; narrow the query '
                    f"(add `rule == '<name>'` if the matrix is shared between rules)"
                )
            return tasks[0]
        raise RemakeError('Give a task key prefix or a -Q query')

    # --- reporting ---

    # MM: got to here so far.
    def status_summary(self, query=None, *, reasons=False, list_tasks=False,
                       list_failures=False):
        """Per-rule task-status summary — the data behind `remake info`.

        Returns {'rules': [...], 'totals': {...}} plus 'tasks' (when
        list_tasks) and 'failures' (when list_failures). Rendering, failure
        grouping and JSON serialisation are the caller's job; this method only
        gathers. Rules are in dependency (topological) order; deferred rules
        appear as {'rule', 'deferred': True} with no counts.

        Counts are a four-state partition of each rule's tasks —
        up_to_date + stale + failed + pending == tasks — where a *stale* task
        succeeded last run but plan() would rerun it (code/uses/io changed or
        an upstream reruns). to_run is the plan's view of the same tasks, so
        up_to_date + to_run == tasks (a success the plan skips is up to date,
        everything else is to run; an adopted-outputs task under
        check_outputs='fallback'/'always' counts as up to date)."""
        import networkx as nx

        if not self._finalized:
            self.finalize()
        # Read-only: the plan and the per-rule status table below ask for the
        # identical record sets — without the cache each rule was queried
        # twice per `remake info` (bug 04 Issue 1).
        self.metadata.ingest_sidecars(self.rules)
        cache = RecordCache(self.metadata)
        runnable, deferred = plan(
            self.rules, self.dag, cache, query=query,
            check_outputs=self.check_outputs,
        )
        remaining = Counter(task.rule.name for task in runnable)
        runnable_keys = {task.key for task in runnable}
        deferred_names = {rule.name for rule in deferred}
        predicate = make_predicate(query, rule_kwarg_names(self.rules)) if query else None

        # Per-rule tally of why the to-run tasks would rerun. One plan() is
        # already done (`runnable`); reuse it per task so this is plan-cost,
        # not N*plan. A task can contribute several categories (e.g. code
        # changed *and* upstream rerun), so counts may exceed the to-run total.
        reasons_by_rule = {}
        if reasons:
            edges, seqs = Edges(), upstream_run_seqs(cache)
            for task in runnable:
                _, rs = explain_task(
                    self.rules, self.dag, cache, task,
                    check_outputs=self.check_outputs, runnable=runnable,
                    edges=edges, upstream_seqs=seqs,
                )
                bucket = reasons_by_rule.setdefault(task.rule.name, Counter())
                for r in rs:
                    bucket[r.category] += 1

        rule_rows, task_rows, failures = [], [], []
        for rule in nx.topological_sort(self.dag):
            if rule.name in deferred_names:
                rule_rows.append({'rule': rule.name, 'deferred': True})
                continue
            tasks = expand_rule(rule, predicate)
            records = cache.get_tasks_status(tasks)
            statuses = {
                t.key: STATUS_NAMES.get(records[t.key].status, 'pending')
                if t.key in records
                else 'pending'
                for t in tasks
            }
            # Status (DB history) crossed with the plan: a success the plan
            # reruns is stale, not up to date; a pending task the plan skips
            # (adopted outputs) is up to date, not pending.
            counts = Counter(
                (statuses[t.key], t.key in runnable_keys) for t in tasks
            )
            row = {
                'rule': rule.name,
                'deferred': False,
                'tasks': len(tasks),
                'up_to_date': counts[('success', False)] + counts[('pending', False)],
                'stale': counts[('success', True)],
                'failed': counts[('failed', True)] + counts[('failed', False)],
                'pending': counts[('pending', True)],
                'to_run': remaining.get(rule.name, 0),
            }
            if reasons:
                row['reasons'] = dict(reasons_by_rule.get(rule.name, {}))
            rule_rows.append(row)
            if list_tasks:
                task_rows.extend(
                    {'task': str(t), 'key': t.key, 'status': statuses[t.key]} for t in tasks
                )
            if list_failures:
                failures.extend(
                    {
                        'task': str(t),
                        'key': t.key,
                        'rule': rule.name,
                        'timestamp': records[t.key].timestamp,
                        'exception': records[t.key].exception,
                        'log': str(task_log_path(t)),
                    }
                    for t in tasks
                    if t.key in records and records[t.key].status == TASK_STATUS_FAILED
                )

        # Bytes of recorded outputs per rule: one aggregate query.
        output_bytes = self.metadata.get_rule_output_bytes([r.name for r in self.rules])
        for row in rule_rows:
            if not row['deferred']:
                row['output_bytes'] = output_bytes.get(row['rule'], 0)

        # Totals across non-deferred rules.
        tallied = [r for r in rule_rows if not r['deferred']]
        totals = {
            field: sum(r[field] for r in tallied)
            for field in ('tasks', 'up_to_date', 'stale', 'failed', 'pending', 'to_run',
                          'output_bytes')
        }
        out = {'rules': rule_rows, 'totals': totals}
        if list_tasks:
            out['tasks'] = task_rows
        if list_failures:
            out['failures'] = failures
        return out

    def task_info(self, task):
        """Detail view of one task as a dict — the data behind `remake
        task-info`: status/timestamp/exception, kwargs, key, input and output
        paths (with exists/complete flags), the per-task log path, and the last
        SLURM submission's job ids + array index."""
        from ..executors.slurm_executor import last_submission

        if not self._finalized:
            self.finalize()
        record = self.metadata.get_tasks_status([task]).get(task.key)
        log_path = task_log_path(task)
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
        for name, token in task.outputs.items():
            if isinstance(token, Dir) and name in outputs:
                # Stale files (not produced by the last run), from the manifest.
                outputs[name]['dir'] = token.summary()
        return {
            'task': str(task),
            'rule': task.rule.name,
            'kwargs': task.kwargs,
            'key': task.key,
            'status': STATUS_NAMES.get(record.status, 'pending') if record else 'pending',
            'timestamp': record.timestamp if record else None,
            'exception': record.exception if record else '',
            # The LAST execution's measured resources — not necessarily the
            # task's current state (set-state does not clear them). All None
            # when never run, never measured, or a pre-0.9 record.
            'resources': {
                'wall_s': record.wall_s if record else None,
                'cpu_s': record.cpu_s if record else None,
                'max_rss_bytes': record.max_rss_bytes if record else None,
                'rss_method': record.rss_method if record else None,
            },
            'inputs': inputs,
            'outputs': outputs,
            # name -> {'n_files', 'total_bytes', 'checksum'} of the last
            # successful run; '*' sums the plain file outputs.
            'output_stats': self.metadata.get_output_stats(task),
            'log': {'path': str(log_path), 'exists': log_path.exists()},
            'slurm': {'jobids': jobids, 'array_index': array_index},
        }

    def task_spec(self, task, *, uses=True):
        """A task's fully resolved spec as a JSON-serialisable dict — the data
        behind `remake export-task`: rule, key, kwargs, resolved input/output
        paths, and where the rule function lives. With uses=True, also the
        rule's `uses` (the value when JSON-serialisable, else its repr)."""
        import json

        if not self._finalized:
            self.finalize()
        rule = task.rule
        fn = rule.fn
        spec = {
            'rule': rule.name,
            'key': task.key,
            'kwargs': task.kwargs,
            'inputs': {k: str(v) for k, v in task.inputs.items()},
            'outputs': {k: str(v) for k, v in task.outputs.items()},
            'function': {
                'module': getattr(fn, '__module__', None),
                'qualname': getattr(fn, '__qualname__', None),
            },
            'remakefile': str(Path(self.remakefile).resolve()) if self.remakefile else None,
        }
        if uses:
            def plain(value):
                try:
                    json.dumps(value)
                    return value
                except (TypeError, ValueError):
                    return repr(value)

            spec['uses'] = {k: plain(v) for k, v in rule.uses.items()}
        return spec

    def rule_from_name(self, name):
        for rule in self.rules:
            if rule.name == name:
                return rule
        raise RemakeError(
            f'No rule named {name!r} (rules: {", ".join(r.name for r in self.rules)})')

    @staticmethod
    def _part_templates(part):
        """Input/output path templates for a rule part, without expanding any
        tasks. A dict part *is* its templates; a callable part is called with
        each kwarg bound to a placeholder that renders as '{case}' but
        supports *only* rendering — any other use of the value (arithmetic,
        format specs, dict lookups) raises, because it would produce a
        template that silently disagrees with the real paths (a plain '{n}'
        string here turned `n * 2` into 'in/{n}{n}.txt'). Returns
        {'templates': {...}}, or {'error': <why>} when not derivable."""
        if part is None:
            return None
        if isinstance(part, dict):
            return {'templates': {k: str(v) for k, v in part.items()}}
        placeholders = {
            p: _TemplatePlaceholder(p) for p in inspect.signature(part).parameters}
        try:
            result = part(**placeholders)
            return {'templates': {k: str(v) for k, v in result.items()}}
        except Exception as e:
            return {'error': f'{type(e).__name__}: {e}'}

    def rule_info(self, rule):
        """Detail view of one rule as a dict — the data behind `remake
        rule-info`: docstring, dependencies (both directions), matrix
        (keys/values/task count, or deferred), input/output path templates
        (derived for callables by passing '{kwarg}' placeholders), uses
        entries (name/kind/rendering, see scope.raw_uses_parts) and config.
        Static introspection only: builds a fresh DAG, touches no metadata."""
        from .dag import resolve_matrix
        from .rule import is_deferrable
        from .scope import raw_uses_parts

        dag = build_rule_dag(self.rules)
        matrix = {
            'kind': ('none' if rule.matrix is None
                     else 'callable' if callable(rule.matrix)
                     else 'list' if isinstance(rule.matrix, list)
                     else 'dict'),
            'deferrable': is_deferrable(rule.matrix),
            'keys': None,
            'n_tasks': None,
            'values': None,
        }
        try:
            rows = resolve_matrix(rule.matrix)
            matrix['n_tasks'] = len(rows)
            keys = []
            for row in rows:
                keys.extend(k for k in row if k not in keys)
            matrix['keys'] = keys
            if isinstance(rule.matrix, dict):
                matrix['values'] = {
                    (k if isinstance(k, str) else '(' + ', '.join(k) + ')'): v
                    for k, v in rule.matrix.items()
                }
        except Defer:
            pass  # dynamic matrix not resolvable yet: keys/n_tasks stay None

        return {
            'rule': rule.name,
            'docstring': inspect.cleandoc(rule.__doc__) if rule.__doc__ else None,
            'depends_on': [dep.name for dep in rule.depends_on],
            'dependents': sorted(s.name for s in dag.successors(rule)),
            'matrix': matrix,
            'inputs': self._part_templates(rule.inputs),
            'outputs': self._part_templates(rule.outputs),
            'uses': [
                {'name': name, 'kind': kind, 'rendering': raw}
                for name, (raw, kind) in raw_uses_parts(rule.uses).items()
            ],
            'config': rule.config,
        }

    def _dir_rules(self):
        """Rules with a Dir output. A dict spec is read directly; only a
        callable spec needs a task resolved to find out."""
        found = []
        for rule in self.rules:
            spec = rule.outputs
            if spec is None:
                continue
            if isinstance(spec, dict):
                if any(isinstance(v, Dir) for v in spec.values()):
                    found.append(rule)
                continue
            try:
                task = next(iter_expand_rule(rule), None)
                if task is not None and any(isinstance(v, Dir) for v in task.outputs.values()):
                    found.append(rule)
            except Exception:  # Defer, or a spec needing runtime state
                continue
        return found

    def _dir_owners(self, rules):
        """{normalised Dir path: first task declaring it}; a second task (or
        a second output of one task) with the same Dir is a RemakeError. Rules
        whose matrix can't be expanded yet are skipped (checked next wave)."""
        owners = {}
        for rule in rules:
            try:
                tasks = list(iter_expand_rule(rule))
            except Exception:  # Defer, or a matrix needing runtime state
                continue
            for task in tasks:
                try:
                    outputs = task.outputs
                except Exception:
                    # An outputs spec that can't resolve for this task now:
                    # run_task records that failure per task (review M11);
                    # this check must not turn it into a crashed run.
                    continue
                for name, token in outputs.items():
                    if not isinstance(token, Dir):
                        continue
                    path = os.path.normpath(token.path)
                    other = owners.setdefault(path, (task, name))
                    if other != (task, name):
                        raise RemakeError(
                            f'Dir {path!r} is declared by two outputs: '
                            f'{other[0]} ({other[1]}) and {task} ({name}). '
                            f'Each Dir belongs to exactly one task.')
        return owners

    def _check_duplicate_dirs(self):
        rules = self._dir_rules()
        if rules:
            self._dir_owners(rules)

    def lint(self):
        """Wiring check — the data behind `remake lint`: every input of a
        dependent rule should be produced by one of its depends_on rules.
        Returns findings rows sorted near-miss-first, each
        {'kind', 'rule', 'other_rule', 'count', 'example'} where kind is
        'near_miss' (input matches a near-identical produced path — a likely
        typo/off-by-one), 'missing_dependency' (input is produced by a rule
        not in depends_on) or 'external' (input produced by no rule). An input
        inside a rule's `Dir` counts as produced by that rule; 'dir_nested'
        (a Dir inside another rule's Dir) and 'file_in_dir' (a plain output
        inside another rule's Dir) are wiring errors too. Two tasks declaring
        one Dir raise RemakeError. Materialises all tasks; rules with an unresolved (deferred) matrix are
        skipped with a warning."""
        import difflib

        if not self._finalized:
            self.finalize()
        self._check_duplicate_dirs()

        producers = {}  # output path -> set of rule names
        rule_outputs = {}  # rule name -> [output paths]
        dir_owner = {}  # normalised Dir path -> rule name
        file_outputs = {}  # normalised plain output path -> rule name
        deferred = set()
        for rule in self.rules:
            paths = []
            try:
                for task in iter_expand_rule(rule):
                    for token in task.outputs.values():
                        paths.append(str(token))
                        if isinstance(token, Dir):
                            dir_owner[os.path.normpath(token.path)] = rule.name
                        elif hasattr(token, '__fspath__'):
                            file_outputs[os.path.normpath(token.path)] = rule.name
            except Defer:
                deferred.add(rule.name)
                logger.warning('{}: matrix not ready, outputs unknown — skipped', rule.name)
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

        def enclosing_dir(path, *, proper=False):
            """The nearest Dir output that contains `path` (or is it, unless
            `proper`): (Dir path, owning rule) or None. A walk up the path's
            parents, so cost is O(depth) per path, not a scan of all Dirs."""
            path = os.path.normpath(path)
            if proper:
                path = os.path.dirname(path)
            while path and path not in ('.', '/'):
                if path in dir_owner:
                    return path, dir_owner[path]
                path = os.path.dirname(path)
            return None

        # Nesting: a Dir inside another task's Dir, or a plain file output
        # inside a Dir, makes "everything in the Dir came from one task" false.
        for path, owner in dir_owner.items():
            if (outer := enclosing_dir(path, proper=True)):
                record('dir_nested', owner, outer[1], {'dir': path, 'inside': outer[0]})
        for path, owner in file_outputs.items():
            if (outer := enclosing_dir(path)) and outer[1] != owner:
                record('file_in_dir', owner, outer[1], {'file': path, 'inside': outer[0]})

        for rule in self.rules:
            if rule.inputs is None or rule.name in deferred:
                continue
            dep_names = {dep.name for dep in rule.depends_on}
            if dep_names & deferred:
                logger.warning('{}: upstream matrix not ready — skipped', rule.name)
                continue
            candidates = [p for name in dep_names for p in rule_outputs.get(name, [])]
            for task in iter_expand_rule(rule):
                for path in map(str, task.inputs.values()):
                    made_by = producers.get(path)
                    if not made_by and dir_owner and (inside := enclosing_dir(path)):
                        # An input inside another rule's Dir is that rule's.
                        made_by = {inside[1]}
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
                            'near_miss', rule.name, producer,
                            {'input': path, 'closest': close[0]},
                        )
                    else:
                        record('external', rule.name, None, path)

        return [
            {'kind': kind, 'rule': rule_name, 'other_rule': other, **entry}
            for (kind, rule_name, other), entry in sorted(
                findings.items(), key=lambda kv: (kv[0][0] != 'near_miss', kv[0])
            )
        ]

    def rule_dag(self, *, with_matrix=False):
        """The rule dependency DAG as data — behind `remake rule-dag`. Returns
        {'order': [rule names, topological], 'edges': {rule: [dependent rule
        names]}}. With `with_matrix`, also 'matrix_info': {rule: (n_tasks,
        keys)}, each (None, None) when a dynamic matrix can't be resolved yet
        (e.g. a continuation rule awaiting upstream outputs). Builds a fresh DAG
        and does not finalize — no metadata backend needed."""
        import networkx as nx

        from .dag import resolve_matrix

        dag = build_rule_dag(self.rules)
        order = list(nx.topological_sort(dag))
        pos = {rule: i for i, rule in enumerate(order)}
        edges = {
            rule.name: [s.name for s in sorted(dag.successors(rule), key=pos.get)]
            for rule in order
        }
        out = {'order': [r.name for r in order], 'edges': edges}
        if with_matrix:
            matrix_info = {}  # rule name -> (n_tasks or None, keys or None)
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
            out['matrix_info'] = matrix_info
        return out

    # --- state ---

    def set_state(self, query, *, success=False, pending=False,
                  check_outputs=False, cascade=True, dry_run=False):
        """Set tasks' recorded state by query, without running them.

        Exactly one of `success`/`pending`. `check_outputs` (success only)
        restricts to tasks whose outputs are complete on disk. `cascade`
        (success only, default on) also re-stamps downstream settled tasks so
        they are not left looking stale. `dry_run` computes the same selection
        but writes nothing.

        Returns {'state', 'tasks', 'cascaded', 'skipped'}: the selected tasks,
        the extra tasks restamped by cascade, and the count dropped by
        check_outputs."""
        if success == pending:
            raise RemakeError('Give exactly one of success / pending')
        if check_outputs and not success:
            raise RemakeError('check_outputs only applies to success')
        if not cascade and not success:
            raise RemakeError('cascade=False only applies to success')

        if not self._finalized:
            self.finalize()
        self.metadata.begin_invocation()  # fresh run_seq for this set-state
        if not dry_run:
            self._record_run('set-state')
        tasks = self.tasks(query=query)
        skipped = 0
        if check_outputs:
            def adoptable(token):
                # A Dir that exists without a manifest is claimed (scan and
                # claim everything present), unless this is a dry run.
                if isinstance(token, Dir):
                    return token.claim(write=not dry_run)
                return token.is_complete()

            verified = [
                t for t in tasks
                if t.outputs and all(adoptable(token) for token in sorted(
                    t.outputs.values(), key=lambda tk: isinstance(tk, Dir)))
            ]
            skipped = len(tasks) - len(verified)
            tasks = verified

        cascaded = self._cascade_descendants(tasks) if (success and cascade) else []

        state = 'success' if success else 'pending'
        if not dry_run:
            if success:
                # One invocation → one run_seq, shared by selected + cascaded, so
                # no intra-batch ordering false-triggers (equal run_seq, strict >).
                self.metadata.update_tasks(tasks + cascaded, TASK_STATUS_SUCCESS)
            else:
                # A PENDING row, not a deleted one: the planner reruns any
                # non-SUCCESS record, and the row keeps the last execution's
                # measured resources (resource_capture.md, Settled 4).
                self.metadata.mark_pending(tasks)
        return {'state': state, 'tasks': tasks, 'cascaded': cascaded, 'skipped': skipped}

    def _cascade_descendants(self, selected_tasks):
        """Downstream SUCCESS tasks to re-stamp alongside `selected_tasks` so the
        settled region stays consistent (see cascade_settled / bug 01)."""
        run_seq, status, task_of = {}, {}, {}
        for rule in self.rules:
            try:
                rtasks = expand_rule(rule)
            except Defer:
                continue
            recs = self.metadata.get_tasks_status(rtasks)
            run_seq[rule], status[rule], task_of[rule] = {}, {}, {}
            for t in rtasks:
                tid = frozenset(t.kwargs.items())
                rec = recs.get(t.key)
                run_seq[rule][tid] = rec.run_seq if rec else None
                status[rule][tid] = rec.status if rec else None
                task_of[rule][tid] = t
        selected = {}
        for t in selected_tasks:
            selected.setdefault(t.rule, set()).add(frozenset(t.kwargs.items()))
        edges = Edges(lambda r: list(task_of[r].values()) if r in task_of else expand_rule(r))
        settled = cascade_settled(set(self.rules), self.dag, selected, run_seq, status, edges)
        cascaded = []
        for rule, ids in settled.items():
            for tid in ids - selected.get(rule, set()):
                cascaded.append(task_of[rule][tid])
        return cascaded

    # --- execution ---

    def run(self, executor=None, query=None, force=False, ignore_code_changes=False):
        """Run all tasks that need running, replanning after each wave so
        dynamic (deferred) matrices resolve as their upstreams complete.
        Returns the number of failed tasks (0 for asynchronous executors,
        which don't know at submission time). Holds `.remake/run.lock` for
        the duration: a second concurrent run in the same directory is a
        RemakeError."""
        if not self._finalized:
            self.finalize()
        self.blocked_rules = []
        if executor is not None and executor.handles_deferred:
            # SLURM: submission only; continuation jobs re-enter run() on
            # compute nodes, and the executor has its own duplicate-submission
            # guard — a directory lock would strand or block them.
            return self._run(executor, query, force, ignore_code_changes)
        with run_lock(self.metadata):
            return self._run(executor, query, force, ignore_code_changes)

    def missing_external_inputs(self, tasks):
        """Inputs of `tasks` that don't exist and that no rule produces —
        {rule name: [paths]} (each distinct path once, in first-seen order).
        Plain paths are checked with os.path.exists, tokens with
        is_complete(). Only inputs of the given tasks are touched, each
        distinct path once; the (all-rules) producer set is built only if
        something is missing. Inputs made by another rule are never
        reported, even if absent now. Tasks that depend on a rule whose
        matrix is still deferred are skipped (their producers are unknown),
        as are tasks whose input spec can't be resolved yet."""
        import os

        checked = {}  # path -> exists
        suspects = []  # (rule, path)
        for task in tasks:
            try:
                values = list(task.inputs.values())
            except Exception:
                continue  # unresolvable here: let the task itself report it
            for value in values:
                path = str(value)
                if not isinstance(value, OutputToken) and _URL_RE.match(path):
                    continue  # a URL (s3://, https://, ...): not ours to stat
                if isinstance(value, OutputToken) and not isinstance(value, PathToken):
                    continue  # remote (S3Object, ...): no network calls at plan time
                if path not in checked:
                    try:
                        if isinstance(value, OutputToken):
                            checked[path] = value.is_complete()
                        else:
                            checked[path] = os.path.exists(path)
                    except Exception:
                        checked[path] = True  # can't tell: not ours to block on
                    if not checked[path]:
                        suspects.append((task.rule, path))
        if not suspects:
            return {}
        from .tokens import Dir

        def canon(p):
            # The same file spelled two ways (relative vs absolute, `./`,
            # a symlinked root) must match (ultrareview 2026-11-21).
            return os.path.realpath(os.path.abspath(p))

        produced = set()
        produced_dirs = set()
        deferred = set()
        for rule in self.rules:
            try:
                for t in iter_expand_rule(rule):
                    for tok in t.outputs.values():
                        norm = canon(str(tok))
                        produced.add(norm)
                        if isinstance(tok, Dir):
                            produced_dirs.add(norm)
            except Exception:
                # Deferred, or an output spec that can't be resolved now:
                # its producers are unknown, so don't blame its consumers.
                deferred.add(rule)

        def is_produced(path):
            norm = canon(path)
            if norm in produced:
                return True
            # A file inside a rule's Dir output is produced by it (as lint
            # treats it).
            parent = os.path.dirname(norm)
            while parent and parent != os.path.dirname(parent):
                if parent in produced_dirs:
                    return True
                parent = os.path.dirname(parent)
            return parent in produced_dirs

        found = {}
        seen = set()
        for rule, path in suspects:
            if is_produced(path) or (path, rule.name) in seen:
                continue
            if deferred and (nx.ancestors(self.dag, rule) & deferred):
                continue
            seen.add((path, rule.name))
            found.setdefault(rule.name, []).append(path)
        return found

    def external_inputs_error(self, found, max_per_rule=5):
        """Message for `missing_external_inputs` output (config
        'external_inputs_hint' appended), or None if nothing is missing."""
        if not found:
            return None
        total = sum(len(v) for v in found.values())
        lines = [f'{total} input file(s) are missing and no rule produces them:']
        for name, paths in found.items():
            lines.append(f'  {name}: {len(paths)} missing')
            for path in paths[:max_per_rule]:
                lines.append(f'    {path}')
            if len(paths) > max_per_rule:
                lines.append(f'    ... and {len(paths) - max_per_rule} more')
        hint = self.config.get('external_inputs_hint')
        if hint:
            lines.append(str(hint))
        return '\n'.join(lines)

    def _check_external_inputs(self, runnable, executor=None, dry_run=False):
        message = self.external_inputs_error(self.missing_external_inputs(runnable))
        if message is None:
            return
        if dry_run or getattr(executor, 'dry_run', False):
            # A dry run reports the problem but still shows the plan.
            logger.warning(message)
            return
        raise RemakeError(message)

    def _run(self, executor, query, force, ignore_code_changes):
        # One run_seq for this whole invocation (shared across replanning
        # waves); committed onto every task so downstream propagation survives
        # to later invocations. See bugs/01_durable_rerun_propagation.md.
        self.metadata.begin_invocation()
        if executor is None:
            from ..executors import SingleprocExecutor

            executor = SingleprocExecutor(self)
        if not getattr(executor, 'dry_run', False):
            self._record_run('run')
        eff = self.effective_config
        logger.info(
            f'Run: executor={type(executor).__name__} '
            f'nproc={getattr(executor, "nproc", None)} '
            f'profile={eff.profile or "none"}')

        def _plan():
            self._check_duplicate_dirs()
            # Records stored by remake <= 0.9.0a1 that still match are
            # re-pointed at the 0.9 renderings, once (core/canonical.py).
            restamp = {}
            planned = self.plan(
                query=query, force=force, ignore_code_changes=ignore_code_changes,
                restamp=restamp,
            )
            if restamp:
                self.metadata.restamp_codes(restamp)
            return planned

        if executor.handles_deferred:
            # Asynchronous executors (SLURM) get the whole plan in one call;
            # deferred rules are theirs to handle (continuation jobs).
            runnable, deferred = _plan()
            if not runnable and not deferred:
                logger.info('Nothing to do')
                return 0
            self._check_external_inputs(runnable, executor)
            return executor.run_tasks(runnable, deferred) or 0

        nfailed = 0
        attempted = set()
        wave = 0
        start = perf_counter()
        while True:
            runnable, deferred = _plan()
            force = False  # only force the first wave
            runnable = [t for t in runnable if t.key not in attempted]
            if not runnable:
                if deferred:
                    # Not everything completed: say which rules and why, and
                    # make the run's outcome reflect it (review 2026-09-24 M9;
                    # it used to be a bare warning and exit 0). A filtered run
                    # may deliberately leave out the upstream a deferred rule
                    # waits on: that one is reported but doesn't fail the run.
                    excused = self._excluded_by_query(deferred) if query else set()
                    for rule in deferred:
                        if rule in excused:
                            logger.warning(f'Blocked rule {rule.name} (upstream not '
                                           f'selected by the query): {_blocked_reason(rule)}')
                        else:
                            self.blocked_rules.append(rule)
                            logger.error(f'Blocked rule {rule.name}: {_blocked_reason(rule)}')
                break
            if wave == 0:
                # First wave only: later waves' inputs come from earlier ones
                # (a failed upstream must not read as a missing external).
                self._check_external_inputs(runnable, executor)
            wave += 1
            logger.bind(event='wave', wave=wave, ntasks=len(runnable)).debug(
                'wave {}: running {} task(s)', wave, len(runnable))
            attempted |= {t.key for t in runnable}
            nfailed += executor.run_tasks(runnable) or 0
        if attempted:
            elapsed = perf_counter() - start
            logger.bind(event='run_summary', ntasks=len(attempted),
                        nfailed=nfailed, nwaves=wave,
                        seconds=round(elapsed, 6)).info(
                'ran {} task(s), {} failed in {:.1f}s',
                len(attempted), nfailed, elapsed)
        else:
            logger.info('Nothing to do')
        return nfailed

    def _excluded_by_query(self, deferred):
        """Deferred rules a filtered run can be excused for: those with an
        upstream (any ancestor) that still has work outside the filter. If
        every ancestor is complete and the rule is still deferred, the query
        didn't cause it — it's genuinely blocked."""
        runnable_all, deferred_all = self.plan()
        unfinished = {t.rule for t in runnable_all} | set(deferred_all)
        return {rule for rule in deferred
                if nx.ancestors(self.dag, rule) & (unfinished - {rule})}

    def run_task(self, task):
        """Execute one task and record the result. The single execution
        entry point — used by all executors and `remake run-task`. Timing and
        completion are logged here so every executor gets them uniformly
        (per-element detail at TRACE, per-task duration at DEBUG — the
        summarise-loops convention, per_task_logging.md)."""
        start = perf_counter()
        capture = None
        # Everything that can fail — output dirs, io resolution (callable
        # specs run here), the function itself — is inside the try, so every
        # failure is recorded with its traceback (review 2026-09-24 M11).
        try:
            # opt(lazy=True): the path lists are only built when a TRACE sink
            # is attached (they'd cost real time at 1e6 tasks otherwise).
            logger.opt(lazy=True).trace(
                'running {}: inputs {} -> outputs {}', lambda: task,
                lambda: [str(p) for p in task.inputs.values()],
                lambda: [str(p) for p in task.outputs.values()],
            )
            for token in task.outputs.values():
                if hasattr(token, '__fspath__'):
                    Path(token).parent.mkdir(parents=True, exist_ok=True)
                token.before_run()

            fn = exec_function(task.rule.fn, task.rule.uses)
            args = []
            if task.rule.inputs is not None:
                args.append(task.inputs)
            if task.rule.outputs is not None:
                args.append(task.outputs)
            # Resources are measured here, the one execution chokepoint every
            # executor shares, so all of them record the same fields
            # (design_docs/designs/resource_capture.md). Both exit paths
            # record: a task that fails after three hours is a duration worth
            # keeping.
            capture = capture_for_config(self.config)
            try:
                with capture:
                    fn(*args, **task.kwargs)
            except SystemExit as exc:
                # sys.exit(0) / sys.exit() from a wrapped CLI main() means
                # success; only a non-zero code is a failure (handled below).
                # Mirror CPython: only None or an int 0 (incl. False) exits 0;
                # sys.exit(0.0) or sys.exit('msg') exit 1.
                if not (exc.code is None or (isinstance(exc.code, int) and exc.code == 0)):
                    raise
            output_stats = self._check_outputs_after_run(task)
        except (Exception, SystemExit) as exc:
            # SystemExit from task code (a CLI main() calling sys.exit) is a
            # task failure, not a request to end the run (review H6).
            # KeyboardInterrupt still propagates: Ctrl-C stops the run.
            tb = traceback.format_exc()
            # Unmeasured (all None) if the task failed before capture began.
            resources = (capture.result() if capture is not None else
                         {'wall_s': None, 'cpu_s': None, 'max_rss_bytes': None,
                          'rss_method': None})
            # Fall back to our own clock when the capture didn't complete:
            # recording the failure matters more than the timing.
            elapsed = resources['wall_s'] or (perf_counter() - start)
            logger.bind(event='task_failed', task=str(task), rule=task.rule.name,
                        key=task.key, seconds=round(elapsed, 6),
                        **_resource_fields(resources),
                        ).error(f'failed: {task} after {elapsed:.2f}s')
            # The traceback at DEBUG: lands in per-task logs (DEBUG sinks)
            # without flooding the INFO console on runs with many failures
            # (review M12).
            logger.debug('traceback for {}:\n{}', task, tb)
            self.metadata.update_task(
                task, TASK_STATUS_FAILED, exception=tb, resources=resources,
            )
            if isinstance(exc, SystemExit):
                raise TaskExit(
                    f'{task} called sys.exit({exc.code!r})'
                ) from exc
            raise
        resources = capture.result()
        elapsed = resources['wall_s']
        logger.bind(event='task_complete', task=str(task), rule=task.rule.name,
                    key=task.key, seconds=round(elapsed, 6),
                    **_resource_fields(resources),
                    ).debug('completed {} in {:.2f}s', task, elapsed)
        # Always passed (an empty dict clears the last run's stats), except to
        # a third-party backend whose update_task predates the keyword.
        extra = ({'output_stats': output_stats}
                 if _accepts_output_stats(self.metadata) else {})
        self.metadata.update_task(task, TASK_STATUS_SUCCESS, resources=resources, **extra)

    def _check_outputs_after_run(self, task):
        """Validate a successful task's outputs (`Ensure`), then collect
        their stats: {output name: {'n_files', 'total_bytes', 'checksum'}}.
        A failed check raises OutputValidationError (the task fails)."""
        problems = [f'{name}: {msg}' for name, token in task.outputs.items()
                    if (msg := token.validate())]
        if problems:
            raise OutputValidationError(
                f'{task}: output check(s) failed:\n  ' + '\n  '.join(problems))
        checksum = bool(task.rule.config.get(
            'checksum_outputs', self.config.get('checksum_outputs', False)))
        stats = {}
        for name, token in task.outputs.items():
            st = token.after_success(checksum=checksum)
            if st is not None:
                stats[name] = st
        return stats
