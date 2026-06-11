"""The planner — decides which tasks need running. Pure except for metadata
reads (injected).

Rerun logic is DB-first: never run, failed, rule_run code changed (AST
compare), uses changed, or any relevant upstream task reruns. Filesystem
checks happen only via the opt-in check_outputs modes.
"""
import networkx as nx

from ..metadata.metadata_manager import TASK_STATUS_SUCCESS
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
