"""The Remake class — wires rules, planner, metadata and executors together."""
import inspect
import traceback
from collections import Counter
from pathlib import Path

from loguru import logger

from ..metadata.metadata_manager import (
    STATUS_NAMES,
    TASK_STATUS_FAILED,
    TASK_STATUS_SUCCESS,
)
from ..util import task_log_path
from .dag import build_rule_dag, expand_rule, iter_expand_rule
from .exceptions import Defer, RemakeError
from .planner import cascade_settled, explain_task, make_predicate, plan
from .rule import Rule
from .scope import check_scope, exec_function
from .task import Task


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
        self.config = config or {}
        self.metadata = metadata
        self.check_outputs = check_outputs
        self.strict_scope = strict_scope
        self.rules = []
        self.dag = None
        self.remakefile = None  # set by load_remake
        self._finalized = False
        if rules:
            self.add_rules(rules)

    # --- registration ---

    def add_rules(self, rules):
        for rule in rules:
            if not isinstance(rule, Rule):
                raise RemakeError(f'Not a Rule (use the @rule decorator): {rule!r}')
            if rule in self.rules:
                continue
            # Resolve tri-state strict_scope against the Remake default.
            if rule.strict_scope is None and self.strict_scope:
                check_scope(rule.fn, rule.uses, strict=True)
            rule.remake = self
            self.rules.append(rule)
        self._finalized = False

    def rules_from_current_module(self):
        # f_locals so rules defined inside functions (tests, notebooks) are
        # found too; at module level locals and globals are the same dict.
        frame = inspect.currentframe().f_back
        namespace = {**frame.f_globals, **frame.f_locals}
        self.add_rules(v for v in namespace.values() if isinstance(v, Rule))

    def rules_from_modules(self, *modules):
        for module in modules:
            self.add_rules(v for v in vars(module).values() if isinstance(v, Rule))

    # --- planning ---

    def finalize(self):
        if self.metadata is None:
            from ..metadata.sqlite3_backend import Sqlite3Backend

            self.metadata = Sqlite3Backend()
        self.dag = build_rule_dag(self.rules)
        self.metadata.ensure_rules(self.rules, remakefile=self.remakefile)
        self._finalized = True
        return self

    def plan(self, query=None, force=False, ignore_code_changes=False):
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
        )

    def explain_task(self, task):
        """(will_run, reasons) for one task — `remake why`."""
        if not self._finalized:
            self.finalize()
        self.metadata.ingest_sidecars(self.rules)
        return explain_task(
            self.rules, self.dag, self.metadata, task, check_outputs=self.check_outputs
        )

    def explain_tasks(self, tasks=None):
        """Yield (task, will_run, reasons) for each task — batch `remake why`.
        Plans once and reuses the runnable set, so cost is one plan() plus
        per-task record/stat checks, not one plan() per task. `tasks=None`
        explains the runnable set itself (bare `remake why`)."""
        if not self._finalized:
            self.finalize()
        self.metadata.ingest_sidecars(self.rules)
        runnable, _ = plan(
            self.rules, self.dag, self.metadata, check_outputs=self.check_outputs
        )
        for task in runnable if tasks is None else tasks:
            will_run, reasons = explain_task(
                self.rules, self.dag, self.metadata, task,
                check_outputs=self.check_outputs, runnable=runnable,
            )
            yield task, will_run, reasons

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
        predicate = make_predicate(query) if query else None
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

    def status_summary(self, query=None, *, reasons=False, list_tasks=False,
                       list_failures=False):
        """Per-rule task-status summary — the data behind `remake info`.

        Returns {'rules': [...], 'totals': {...}} plus 'tasks' (when
        list_tasks) and 'failures' (when list_failures). Rendering, failure
        grouping and JSON serialisation are the caller's job; this method only
        gathers. Rules are in dependency (topological) order; deferred rules
        appear as {'rule', 'deferred': True} with no counts."""
        import networkx as nx

        if not self._finalized:
            self.finalize()
        runnable, deferred = self.plan(query=query)
        remaining = Counter(task.rule.name for task in runnable)
        deferred_names = {rule.name for rule in deferred}
        predicate = make_predicate(query) if query else None

        # Per-rule tally of why the to-run tasks would rerun. One plan() is
        # already done (`runnable`); reuse it per task so this is plan-cost,
        # not N*plan. A task can contribute several categories (e.g. code
        # changed *and* upstream rerun), so counts may exceed the to-run total.
        reasons_by_rule = {}
        if reasons:
            for task in runnable:
                _, rs = explain_task(
                    self.rules, self.dag, self.metadata, task,
                    check_outputs=self.check_outputs, runnable=runnable,
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
            records = self.metadata.get_tasks_status(tasks)
            statuses = {
                t.key: STATUS_NAMES.get(records[t.key].status, 'pending')
                if t.key in records
                else 'pending'
                for t in tasks
            }
            counts = Counter(statuses.values())
            row = {
                'rule': rule.name,
                'deferred': False,
                'tasks': len(tasks),
                'success': counts['success'],
                'failed': counts['failed'],
                'pending': counts['pending'],
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
                        'timestamp': records[t.key].timestamp,
                        'exception': records[t.key].exception,
                        'log': str(task_log_path(t)),
                    }
                    for t in tasks
                    if t.key in records and records[t.key].status == TASK_STATUS_FAILED
                )

        # Totals across non-deferred rules.
        tallied = [r for r in rule_rows if not r['deferred']]
        totals = {
            field: sum(r[field] for r in tallied)
            for field in ('tasks', 'success', 'failed', 'pending', 'to_run')
        }
        out = {'rules': rule_rows, 'totals': totals}
        if list_tasks:
            out['tasks'] = task_rows
        if list_failures:
            out['failures'] = failures
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
        tasks = self.tasks(query=query)
        skipped = 0
        if check_outputs:
            verified = [
                t for t in tasks
                if t.outputs and all(token.is_complete() for token in t.outputs.values())
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
                self.metadata.delete_tasks(tasks)
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
        settled = cascade_settled(set(self.rules), self.dag, selected, run_seq, status)
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
        which don't know at submission time)."""
        if not self._finalized:
            self.finalize()
        # One run_seq for this whole invocation (shared across replanning
        # waves); committed onto every task so downstream propagation survives
        # to later invocations. See bugs/01_durable_rerun_propagation.md.
        self.metadata.begin_invocation()
        if executor is None:
            from ..executors import SingleprocExecutor

            executor = SingleprocExecutor(self)

        def _plan():
            return self.plan(
                query=query, force=force, ignore_code_changes=ignore_code_changes
            )

        if executor.handles_deferred:
            # Asynchronous executors (SLURM) get the whole plan in one call;
            # deferred rules are theirs to handle (continuation jobs).
            runnable, deferred = _plan()
            if not runnable and not deferred:
                logger.info('Nothing to do')
                return 0
            return executor.run_tasks(runnable, deferred) or 0

        nfailed = 0
        attempted = set()
        wave = 0
        while True:
            runnable, deferred = _plan()
            force = False  # only force the first wave
            runnable = [t for t in runnable if t.key not in attempted]
            if not runnable:
                if deferred:
                    names = ', '.join(rule.name for rule in deferred)
                    logger.warning(f'Blocked rules (matrix not ready): {names}')
                break
            wave += 1
            logger.debug('wave {}: running {} task(s)', wave, len(runnable))
            attempted |= {t.key for t in runnable}
            nfailed += executor.run_tasks(runnable) or 0
        return nfailed

    def run_task(self, task):
        """Execute one task and record the result. The single execution
        entry point — used by all executors and `remake run-task`."""
        for token in task.outputs.values():
            if hasattr(token, '__fspath__'):
                Path(token).parent.mkdir(parents=True, exist_ok=True)

        fn = exec_function(task.rule.fn, task.rule.uses)
        args = []
        if task.rule.inputs is not None:
            args.append(task.inputs)
        if task.rule.outputs is not None:
            args.append(task.outputs)
        try:
            fn(*args, **task.kwargs)
        except Exception:
            logger.error(f'failed: {task}')
            self.metadata.update_task(
                task, TASK_STATUS_FAILED, exception=traceback.format_exc()
            )
            raise
        self.metadata.update_task(task, TASK_STATUS_SUCCESS)
