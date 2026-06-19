"""The planner — decides which tasks need running. Pure except for metadata
reads (injected).

Rerun logic is DB-first: never run, failed, rule_run code changed (AST
compare), uses changed, or any relevant upstream task reruns. Filesystem
checks happen only via the opt-in check_outputs modes.
"""
import difflib
from collections import namedtuple
from time import perf_counter

import networkx as nx
from loguru import logger

from ..metadata.metadata_manager import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from ..util.code_compare import CodeComparer
from .dag import expand_rule
from .exceptions import Defer
from .rule import is_deferrable
from .scope import io_hash, parse_uses_hash, uses_hash, uses_parts


def make_predicate(query):
    """Compile a task-filter expression evaluated against task kwargs plus
    'rule' (the rule name), e.g. "year > 1985 and model == 'era5'",
    "rule in ['extract', 'clean']"."""
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


def _upstream_rerunning(rule, rerun_kwargs):
    """Any depends_on upstream rerunning this wave? An entry is 'all'
    (truthy) or a set of kwarg-tuples (truthy when non-empty); a rule that
    fully passes leaves an empty set (falsy)."""
    return any(rerun_kwargs.get(dep) for dep in rule.depends_on)


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


# A reason carries a short `category` (for aggregate rollups like
# `info --reasons`) and the human `message` (`remake why`). Reason is
# str-compatible enough for printing via its message; consumers that want the
# bucket read `.category`.
Reason = namedtuple('Reason', 'category message')


def _plain_rendering(rendered):
    """A `uses` value rendered as a plain `repr` (showable inline) rather
    than a callable's AST-normalised source (an `ast.dump`, single-line but
    starting with `Module(`) or a multi-line source fallback (e.g. a lambda
    whose source didn't parse standalone)."""
    return '\n' not in rendered and not rendered.startswith('Module(')


def _uses_change_message(stored_hash, uses):
    """Human description of which `uses` keys changed between the stored hash
    and the current `uses` dict. Names the keys; for plain (repr) values it
    shows before → after, for callables it says (body)."""
    old = parse_uses_hash(stored_hash)
    new = uses_parts(uses)
    bits = []
    for name in sorted(set(old) | set(new)):
        if name not in new:
            bits.append(f'{name} (removed)')
        elif name not in old:
            bits.append(f'{name} (added)')
        elif old[name] != new[name]:
            if _plain_rendering(old[name]) and _plain_rendering(new[name]):
                bits.append(f'{name}: {old[name]} → {new[name]}')
            else:
                bits.append(f'{name} (body)')
    return 'uses= changed since last run: ' + ', '.join(bits)


def explain_task(rules, dag, metadata, task, *, check_outputs='never', runnable=None):
    """Why would (or wouldn't) this task run? Returns (will_run, reasons),
    each reason a `Reason(category, message)` in the order the planner checks
    them. The `remake why` command (messages) and `info --reasons` (categories).

    `runnable` is the precomputed `plan()` runnable list; pass it to explain
    many tasks without re-planning per task (one plan() shared across them).
    Computed internally when not supplied (the single-task case)."""
    if runnable is None:
        runnable, _ = plan(rules, dag, metadata, check_outputs=check_outputs)
    will_run = any(t.key == task.key for t in runnable)

    reasons = []
    rec = metadata.get_tasks_status([task]).get(task.key)
    if rec is None:
        if check_outputs in ('fallback', 'always') and _outputs_complete(task):
            reasons.append(Reason('adopted-outputs',
                f'never recorded in the DB, but all outputs are complete on disk '
                f'(check_outputs={check_outputs!r} adopts them)'))
        else:
            reasons.append(Reason('never-run', 'never run (no DB record)'))
    else:
        if rec.status != TASK_STATUS_SUCCESS:
            state = 'failed' if rec.status == TASK_STATUS_FAILED else 'pending'
            reasons.append(Reason(f'last-run-{state}', f'last run {state} at {rec.timestamp}'))
        run_src = task.rule.source['run']
        if not CodeComparer()(rec.run_code, run_src):
            diff = '\n'.join(
                difflib.unified_diff(
                    rec.run_code.splitlines(), run_src.splitlines(),
                    'last run', 'current', lineterm='',
                )
            )
            reasons.append(Reason('code-changed', f'run code changed since last run:\n{diff}'))
        if rec.uses_hash != uses_hash(task.rule.uses):
            reasons.append(Reason('uses-changed',
                _uses_change_message(rec.uses_hash, task.rule.uses)))
        if rec.io_hash is not None and rec.io_hash != io_hash(task.rule):
            reasons.append(Reason('io-changed',
                'inputs/outputs spec changed since last run'))
        if check_outputs == 'always' and task.outputs and not _outputs_complete(task):
            reasons.append(Reason('outputs-missing',
                'outputs missing/incomplete (check_outputs=always)'))

    for dep in task.rule.depends_on:
        dep_running = [t for t in runnable if t.rule is dep]
        if not dep_running:
            continue
        if _same_matrix(task.rule, dep):
            match = [t for t in dep_running if t.kwargs == task.kwargs]
            if match:
                reasons.append(Reason('upstream-rerun',
                    f'upstream {match[0]} reruns (shared matrix: element-wise)'))
        else:
            reasons.append(Reason('upstream-rerun',
                f'{len(dep_running)} upstream {dep.name} task(s) rerun '
                f'(different matrix: conservative, all downstream tasks rerun)'))

    return will_run, reasons


def plan(rules, dag, metadata, *, query=None, force=False, check_outputs='never',
         ignore_code_changes=False):
    """Return (runnable_tasks, deferred_rules).

    runnable_tasks: ordered (rule-topologically) list of tasks needing a run.
    deferred_rules: rules deferred this wave — a @deferrable matrix that
    raised Defer (upstream output absent) or whose upstream is rerunning
    (output stale), plus anything downstream of a deferred rule.

    ignore_code_changes: freshness checks off, dataflow on — code/uses
    comparisons are skipped, so a task reruns only if it has never
    *succeeded* (failed counts as not run) or an upstream task reruns
    this wave (a fan-in must still pick up newly-run elements).
    """
    start = perf_counter()
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
            logger.debug('{}: deferred (downstream of a deferred rule)', rule.name)
            deferred.append(rule)
            rerun_kwargs[rule] = 'all'
            continue
        if is_deferrable(rule.matrix) and _upstream_rerunning(rule, rerun_kwargs):
            # A @deferrable matrix derives its task set from upstream outputs.
            # If an upstream is rerunning this wave its on-disk output is stale,
            # so expanding now would build the wrong task set. Defer: the local
            # replan loop re-expands after the upstream finishes; the SLURM
            # continuation job re-plans it with fresh outputs.
            logger.debug(
                '{}: deferred (deferrable matrix, upstream rerunning)', rule.name
            )
            deferred.append(rule)
            rerun_kwargs[rule] = 'all'
            continue
        try:
            tasks = expand_rule(rule, predicate)
        except Defer:
            logger.debug('{}: deferred (matrix not ready)', rule.name)
            deferred.append(rule)
            # Unknown tasks: downstream rules must assume everything reruns.
            rerun_kwargs[rule] = 'all'
            continue

        records = metadata.get_tasks_status(tasks)
        run_src = rule.source['run']
        current_uses_hash = uses_hash(rule.uses)
        current_io_hash = io_hash(rule)
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
            # `reason` is a short literal (cheap to assign every iteration);
            # only formatted into a log line when a TRACE sink is attached.
            if rec is None:
                if check_outputs in ('fallback', 'always') and _outputs_complete(task):
                    rerun, reason = False, 'outputs complete (no DB record)'
                else:
                    rerun, reason = True, 'never run (no DB record)'
            else:
                rerun = rec.status != TASK_STATUS_SUCCESS
                reason = 'last run not successful' if rerun else 'up to date'
                if not rerun and not ignore_code_changes:
                    if not code_comparer(rec.run_code, run_src):
                        rerun, reason = True, 'run code changed'
                    elif rec.uses_hash != current_uses_hash:
                        rerun, reason = True, 'uses= changed'
                    # io_hash is None for pre-upgrade records — don't rerun on
                    # that alone (treat as not-yet-tracked, not as changed).
                    elif rec.io_hash is not None and rec.io_hash != current_io_hash:
                        rerun, reason = True, 'inputs/outputs spec changed'
                if not rerun and check_outputs == 'always' and task.outputs:
                    if not _outputs_complete(task):
                        rerun, reason = True, 'outputs missing (check_outputs=always)'
            if force:
                rerun, reason = True, 'forced'

            if not rerun:
                task_id = frozenset(task.kwargs.items())
                if upstream_all or any(task_id in dep_rerun for dep_rerun in elementwise_deps):
                    rerun, reason = True, 'upstream reruns'

            logger.trace('{}: {} — {}', task.key, 'rerun' if rerun else 'skip', reason)
            if rerun:
                rule_rerun.add(frozenset(task.kwargs.items()))
                runnable.append(task)

        rerun_kwargs[rule] = rule_rerun
        logger.debug('{}: {} task(s), {} to rerun', rule.name, len(tasks), len(rule_rerun))

    logger.debug(
        'plan: {} runnable, {} deferred in {:.3f}s',
        len(runnable), len(deferred), perf_counter() - start,
    )
    return runnable, deferred
