"""The Remake class — wires rules, planner, metadata and executors together."""
import inspect
from pathlib import Path

from loguru import logger

from ..metadata.metadata_manager import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from .dag import build_rule_dag, expand_rule
from .exceptions import RemakeError
from .planner import make_predicate, plan
from .rule import Rule
from .scope import check_scope, exec_function


class Remake:
    def __init__(
        self,
        *,
        rules=None,
        config=None,
        metadata=None,
        check_outputs='fallback',
        strict_scope=False,
    ):
        self.config = config or {}
        self.metadata = metadata
        self.check_outputs = check_outputs
        self.strict_scope = strict_scope
        self.rules = []
        self.dag = None
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
        caller_globals = inspect.currentframe().f_back.f_globals
        self.add_rules(v for v in caller_globals.values() if isinstance(v, Rule))

    def rules_from_modules(self, *modules):
        for module in modules:
            self.add_rules(v for v in vars(module).values() if isinstance(v, Rule))

    # --- planning ---

    def finalize(self):
        if self.metadata is None:
            from ..metadata.sqlite3_backend import Sqlite3Backend

            self.metadata = Sqlite3Backend()
        self.dag = build_rule_dag(self.rules)
        self.metadata.ensure_rules(self.rules)
        self._finalized = True
        return self

    def plan(self, query=None, force=False):
        if not self._finalized:
            self.finalize()
        return plan(
            self.rules,
            self.dag,
            self.metadata,
            query=query,
            force=force,
            check_outputs=self.check_outputs,
        )

    def tasks(self, query=None):
        """All tasks of all currently-expandable rules. This materialises
        every task — intended for info/reporting, not planning."""
        if not self._finalized:
            self.finalize()
        predicate = make_predicate(query) if query else None
        tasks = []
        for rule in self.rules:
            tasks.extend(expand_rule(rule, predicate))
        return tasks

    def task_from_key(self, key):
        for task in self.tasks():
            if task.key == key:
                return task
        raise RemakeError(f'No task with key {key}')

    # --- execution ---

    def run(self, executor=None, query=None, force=False):
        """Run all tasks that need running, replanning after each wave so
        dynamic (deferred) matrices resolve as their upstreams complete."""
        if not self._finalized:
            self.finalize()
        if executor is None:
            from ..executors import SingleprocExecutor

            executor = SingleprocExecutor(self)

        attempted = set()
        while True:
            runnable, deferred = self.plan(query=query, force=force)
            force = False  # only force the first wave
            runnable = [t for t in runnable if t.key not in attempted]
            if not runnable:
                if deferred:
                    names = ', '.join(rule.name for rule in deferred)
                    logger.warning(f'Blocked rules (matrix not ready): {names}')
                break
            attempted |= {t.key for t in runnable}
            executor.run_tasks(runnable)

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
        except Exception as e:
            logger.error(f'failed: {task}')
            self.metadata.update_task(task, TASK_STATUS_FAILED, exception=repr(e))
            raise
        self.metadata.update_task(task, TASK_STATUS_SUCCESS)
