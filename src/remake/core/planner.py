"""The planner — decides which tasks need running. Pure except for metadata
reads (injected).

Rerun logic is DB-first: never run, failed, rule_run code changed (AST
compare), uses changed, or any relevant upstream task reruns. Filesystem
checks happen only via the opt-in check_outputs modes.
"""
import difflib

import networkx as nx

from ..metadata.metadata_manager import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from ..util.code_compare import CodeComparer
from .dag import expand_rule
from .exceptions import MatrixNotReady
from .scope import uses_hash


def make_predicate(query):
    """Compile a task-filter expression evaluated against task kwargs,
    e.g. "year > 1985 and model == 'era5'"."""
    # MM: this looks like a risk - using compile to compile the code?
    # See how pyquerylist does this - it only allows certain Python ops.
    code = compile(query, '<query>', 'eval')

    def predicate(kwargs):
        try:
            return bool(eval(code, {'__builtins__': {}}, dict(kwargs)))
        except NameError:
            # Query references a kwarg this rule doesn't have: no match.
            return False

    return predicate


def _same_matrix(rule, dep):
    """Element-wise rerun propagation applies when a rule shares its
    upstream's matrix (the matrix=upstream.matrix idiom)."""
    return rule.matrix is dep.matrix or rule.matrix == dep.matrix


def _outputs_complete(task):
    outputs = task.outputs
    return bool(outputs) and all(token.is_complete() for token in outputs.values())


def upstream_failed(task, failures):
    """Should task be skipped because upstream tasks failed this run?

    failures: {rule: set of frozenset(kwargs.items())} accumulated by an
    executor. Mirrors the planner's rerun propagation: element-wise when
    the matrices are shared, conservative (any failure taints all
    downstream tasks) otherwise.
    """
    for dep in task.rule.depends_on:
        failed = failures.get(dep)
        if not failed:
            continue
        if _same_matrix(task.rule, dep):
            if frozenset(task.kwargs.items()) in failed:
                return True
        else:
            return True
    return False


def explain_task(rules, dag, metadata, task, *, check_outputs='fallback'):
    """Why would (or wouldn't) this task run? Returns (will_run, reasons) —
    reasons in the order the planner checks them. The `remake why` command."""
    runnable, _ = plan(rules, dag, metadata, check_outputs=check_outputs)
    will_run = any(t.key == task.key for t in runnable)

    reasons = []
    rec = metadata.get_tasks_status([task]).get(task.key)
    if rec is None:
        if check_outputs in ('fallback', 'always') and _outputs_complete(task):
            reasons.append(
                f'never recorded in the DB, but all outputs are complete on disk '
                f'(check_outputs={check_outputs!r} adopts them)'
            )
        else:
            reasons.append('never run (no DB record)')
    else:
        if rec.status != TASK_STATUS_SUCCESS:
            state = 'failed' if rec.status == TASK_STATUS_FAILED else 'pending'
            reasons.append(f'last run {state} at {rec.timestamp}')
        run_src = task.rule.source['run']
        if not CodeComparer()(rec.run_code, run_src):
            diff = '\n'.join(
                difflib.unified_diff(
                    rec.run_code.splitlines(), run_src.splitlines(),
                    'last run', 'current', lineterm='',
                )
            )
            reasons.append(f'run code changed since last run:\n{diff}')
        if rec.uses_hash != uses_hash(task.rule.uses):
            reasons.append('uses= changed since last run')
        if check_outputs == 'always' and task.outputs and not _outputs_complete(task):
            reasons.append('outputs missing/incomplete (check_outputs=always)')

    for dep in task.rule.depends_on:
        dep_running = [t for t in runnable if t.rule is dep]
        if not dep_running:
            continue
        if _same_matrix(task.rule, dep):
            match = [t for t in dep_running if t.kwargs == task.kwargs]
            if match:
                reasons.append(f'upstream {match[0]} reruns (shared matrix: element-wise)')
        else:
            reasons.append(
                f'{len(dep_running)} upstream {dep.name} task(s) rerun '
                f'(different matrix: conservative, all downstream tasks rerun)'
            )

    return will_run, reasons


def plan(rules, dag, metadata, *, query=None, force=False, check_outputs='fallback'):
    """Return (runnable_tasks, deferred_rules).

    runnable_tasks: ordered (rule-topologically) list of tasks needing a run.
    deferred_rules: rules whose matrix callable raised MatrixNotReady.
    """
    predicate = make_predicate(query) if query else None
    code_comparer = CodeComparer()
    rules = set(rules)

    runnable = []
    deferred = []
    rerun_kwargs = {}  # rule -> set of frozenset(kwargs.items()), or 'all'

    for rule in nx.topological_sort(dag):
        if rule not in rules:
            continue
        if any(dep in deferred for dep in rule.depends_on):
            # Downstream of a deferred rule: cannot run this wave even if
            # its own matrix is static — its upstream tasks don't exist yet.
            deferred.append(rule)
            rerun_kwargs[rule] = 'all'
            continue
        try:
            tasks = expand_rule(rule, predicate)
        except MatrixNotReady:
            deferred.append(rule)
            # Unknown tasks: downstream rules must assume everything reruns.
            rerun_kwargs[rule] = 'all'
            continue

        records = metadata.get_tasks_status(tasks)
        run_src = rule.source['run']
        current_uses_hash = uses_hash(rule.uses)
        rule_rerun = set()

        upstream_all = any(rerun_kwargs.get(dep) == 'all' for dep in rule.depends_on)
        elementwise_deps = []
        for dep in rule.depends_on:
            dep_rerun = rerun_kwargs.get(dep, set())
            if dep_rerun == 'all' or not dep_rerun:
                continue
            if _same_matrix(rule, dep):
                elementwise_deps.append(dep_rerun)
            else:
                # Fan-in or differing matrices: conservative.
                upstream_all = True

        for task in tasks:
            rec = records.get(task.key)
            if rec is None:
                if check_outputs in ('fallback', 'always') and _outputs_complete(task):
                    rerun = False  # recognised from outputs; DB record absent
                else:
                    rerun = True
            else:
                rerun = (
                    rec.status != TASK_STATUS_SUCCESS
                    or not code_comparer(rec.run_code, run_src)
                    or rec.uses_hash != current_uses_hash
                )
                if not rerun and check_outputs == 'always' and task.outputs:
                    rerun = not _outputs_complete(task)
            if force:
                rerun = True

            if not rerun:
                task_id = frozenset(task.kwargs.items())
                rerun = upstream_all or any(task_id in dep_rerun for dep_rerun in elementwise_deps)

            if rerun:
                rule_rerun.add(frozenset(task.kwargs.items()))
                runnable.append(task)

        rerun_kwargs[rule] = rule_rerun

    return runnable, deferred
