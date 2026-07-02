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
from .scope import io_hash, parse_uses_hash, raw_uses_parts, uses_hash, uses_parts


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


def _max_upstream_run_seq(rule, task_kwargs, run_seq_by_rule):
    """Highest run_seq among the upstream tasks feeding this task — element-wise
    when the matrices match, else the max over all of the upstream rule's tasks
    (conservative, mirroring rerun propagation). `run_seq_by_rule` maps a rule
    to {frozenset(kwargs.items()): run_seq or None}. Returns None when no
    upstream run_seq is known (nothing to compare against)."""
    best = None
    for dep in rule.depends_on:
        seqs = run_seq_by_rule.get(dep, {})
        candidates = [seqs.get(task_kwargs)] if _same_matrix(rule, dep) else seqs.values()
        for s in candidates:
            if s is not None and (best is None or s > best):
                best = s
    return best


def _outputs_complete(task):
    outputs = task.outputs
    return bool(outputs) and all(token.is_complete() for token in outputs.values())


def cascade_settled(rule_set, dag, selected, run_seq, status):
    """Guarded downstream cascade for `set-state --success`.

    Stamping a task with the current (highest) run_seq makes it newer than its
    descendants, which would then look stale and rerun. To keep a settled
    region self-consistent, the cascade also re-stamps downstream tasks — but
    *guards* against a descendant that has an independently-newer upstream
    (a diamond where another branch genuinely changed): re-stamping it would
    swallow that branch's propagation. See
    design_docs/bugs/01_durable_rerun_propagation.md.

    Inputs are materialised maps keyed by rule then frozenset(kwargs.items()):
    `selected` (the user's chosen task-ids, the settle seed), `run_seq`
    (task-id -> run_seq or None), `status` (task-id -> status int). Returns the
    full settled set {rule: set(task_kwargs)} = selected + cascaded; only already
    SUCCESS descendants are ever added (never-run/failed are left to rerun).

    The cascade is local and needs no subtree pruning: a descendant skipped by
    the guard reruns through normal propagation and re-stamps higher when it
    does, re-triggering its own descendants on the next pass.
    """
    settled = {rule: set(ids) for rule, ids in selected.items()}
    for rule in nx.topological_sort(dag):
        if rule not in rule_set or not rule.depends_on:
            continue
        for task_kwargs, st in status.get(rule, {}).items():
            if st != TASK_STATUS_SUCCESS or task_kwargs in settled.get(rule, set()):
                continue
            this_seq = run_seq.get(rule, {}).get(task_kwargs)
            downstream_of_settled = independent_newer = False
            for dep in rule.depends_on:
                dep_ids = ([task_kwargs] if _same_matrix(rule, dep)
                           else list(run_seq.get(dep, {})))
                for did in dep_ids:
                    if did in settled.get(dep, set()):
                        downstream_of_settled = True
                    else:
                        dseq = run_seq.get(dep, {}).get(did)
                        if dseq is not None and (this_seq is None or dseq > this_seq):
                            independent_newer = True
            if downstream_of_settled and not independent_newer:
                settled.setdefault(rule, set()).add(task_kwargs)
    return settled


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


def _uses_change_message(stored_hash, uses, old_manifest=None):
    """Human description of which `uses` keys changed between the stored hash
    and the current `uses` dict. Names the keys; for plain (repr) values it
    shows before → after. For callables: a raw-source unified diff when the
    old version's source is on record (`old_manifest`, from
    `metadata.get_uses_manifest` — {name: (raw, kind)}); "source unavailable"
    for sourceless (bytecode-tracked) callables; bare "(body)" when the old
    raw source is unknown (records predating the manifest table)."""
    old = parse_uses_hash(stored_hash)
    new = uses_parts(uses)
    old_manifest = old_manifest or {}
    new_raw = raw_uses_parts(uses) if old_manifest else {}
    bits = []
    for name in sorted(set(old) | set(new)):
        if name not in new:
            bits.append(f'{name} (removed)')
        elif name not in old:
            bits.append(f'{name} (added)')
        elif old[name] != new[name]:
            if _plain_rendering(old[name]) and _plain_rendering(new[name]):
                bits.append(f'{name}: {old[name]} → {new[name]}')
                continue
            old_raw, old_kind = old_manifest.get(name, (None, None))
            cur_raw, cur_kind = new_raw.get(name, (None, None))
            if old_kind == 'source' and cur_kind == 'source':
                diff = '\n'.join(difflib.unified_diff(
                    old_raw.splitlines(), cur_raw.splitlines(),
                    'last run', 'current', lineterm=''))
                bits.append(f'{name} (body):\n{diff}')
            elif old_kind == 'value' and cur_kind == 'value':
                # A multi-line repr (single-line ones took the branch above).
                bits.append(f'{name}: {old_raw} → {cur_raw}')
            elif 'bytecode' in (old_kind, cur_kind):
                bits.append(f'{name} (body; source unavailable)')
            else:
                bits.append(f'{name} (body)')
    if any('\n' in bit for bit in bits):
        return 'uses= changed since last run:\n' + '\n'.join(bits)
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
        # Records carry code ids; resolve this one task's stored texts (the
        # lazy fetch — only `why` pays for the full source, never the planner).
        codes = metadata.get_codes(
            {rec.run_code_id, rec.uses_code_id, rec.io_code_id})
        run_src = task.rule.source['run']
        stored_run = codes.get(rec.run_code_id) or ''
        if not CodeComparer()(stored_run, run_src):
            diff = '\n'.join(
                difflib.unified_diff(
                    stored_run.splitlines(), run_src.splitlines(),
                    'last run', 'current', lineterm='',
                )
            )
            reasons.append(Reason('code-changed', f'run code changed since last run:\n{diff}'))
        stored_uses = codes.get(rec.uses_code_id) or ''
        if stored_uses != uses_hash(task.rule.uses):
            old_manifest = (metadata.get_uses_manifest(rec.uses_code_id)
                            if rec.uses_code_id is not None else {})
            reasons.append(Reason('uses-changed',
                _uses_change_message(stored_uses, task.rule.uses, old_manifest)))
        if rec.io_code_id is not None and codes.get(rec.io_code_id) != io_hash(task.rule):
            reasons.append(Reason('io-changed',
                'inputs/outputs spec changed since last run'))
        if check_outputs == 'always' and task.outputs and not _outputs_complete(task):
            reasons.append(Reason('outputs-missing',
                'outputs missing/incomplete (check_outputs=always)'))

    in_pass_upstream = False
    for dep in task.rule.depends_on:
        dep_running = [t for t in runnable if t.rule is dep]
        if not dep_running:
            continue
        if _same_matrix(task.rule, dep):
            match = [t for t in dep_running if t.kwargs == task.kwargs]
            if match:
                in_pass_upstream = True
                reasons.append(Reason('upstream-rerun',
                    f'upstream {match[0]} reruns (shared matrix: element-wise)'))
        else:
            in_pass_upstream = True
            reasons.append(Reason('upstream-rerun',
                f'{len(dep_running)} upstream {dep.name} task(s) rerun '
                f'(different matrix: conservative, all downstream tasks rerun)'))

    # Durable cross-pass propagation: an upstream was committed in a later
    # invocation than this task without rerunning it in the same pass (the gap
    # that an in-pass-only signal misses). Only reported when nothing upstream
    # is rerunning *this* pass — otherwise the upstream-rerun reason above is
    # the live cause. Mirrors the planner's `_max_upstream_run_seq` check.
    if rec is not None and rec.run_seq is not None and not in_pass_upstream:
        run_seq_by_rule = {}
        for dep in task.rule.depends_on:
            try:
                dep_tasks = expand_rule(dep)
            except Defer:
                continue
            dep_recs = metadata.get_tasks_status(dep_tasks)
            run_seq_by_rule[dep] = {
                frozenset(t.kwargs.items()):
                    (dep_recs[t.key].run_seq if t.key in dep_recs else None)
                for t in dep_tasks
            }
        up_seq = _max_upstream_run_seq(
            task.rule, frozenset(task.kwargs.items()), run_seq_by_rule)
        if up_seq is not None and up_seq > rec.run_seq:
            reasons.append(Reason('upstream-newer',
                f'an upstream ran more recently (run_seq {up_seq} > {rec.run_seq}) '
                f'without rerunning this task — output may be stale'))

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
    # MM: this is a core piece of logic, but I find it hard to understand end-to-end.
    # MM: also quite a long func.
    start = perf_counter()
    predicate = make_predicate(query) if query else None
    code_comparer = CodeComparer()
    rules = set(rules)

    runnable = []
    deferred = []
    rerun_kwargs = {}  # rule -> set of frozenset(kwargs.items()), or 'all'
    # rule -> {frozenset(kwargs.items()): run_seq or None}. Threaded in topo
    # order so a task can compare its stored run_seq against its upstreams'
    # (durable cross-pass propagation; see bugs/01_durable_rerun_propagation.md).
    task_run_seq = {}

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

        # Records carry code *ids*, not text; resolve the distinct few (per
        # rule, typically 1 of each — more only when tasks last ran under
        # different code versions) and compare each against the current state
        # once. The per-task check below is then set membership on ints —
        # this is what keeps status+plan cost from scaling with task count
        # (logs_analysis §1.1/1.2).
        run_ids = {rec.run_code_id for rec in records.values()}
        uses_ids = {rec.uses_code_id for rec in records.values()}
        io_ids = {rec.io_code_id for rec in records.values()}
        codes = metadata.get_codes(run_ids | uses_ids | io_ids)
        run_unchanged = {cid for cid in run_ids
                         if code_comparer(codes.get(cid) or '', run_src)}
        uses_unchanged = {cid for cid in uses_ids
                          if (codes.get(cid) or '') == current_uses_hash}
        # io id None = pre-upgrade record, not tracked: never a rerun cause.
        io_unchanged = {cid for cid in io_ids
                        if cid is None or codes.get(cid) == current_io_hash}

        # MM: what does this block do?
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
            task_kwargs = frozenset(task.kwargs.items())
            # `reason` is a short literal (cheap to assign every iteration);
            # only formatted into a log line when a TRACE sink is attached.
            if rec is None:
                if check_outputs in ('fallback', 'always') and _outputs_complete(task):
                    rerun, reason = False, 'outputs complete (no DB record)'
                else:
                    rerun, reason = True, 'never run (no DB record)'
            else:
                # MM: multiple of these could be true, and this is not recorded.
                rerun = rec.status != TASK_STATUS_SUCCESS
                reason = 'last run not successful' if rerun else 'up to date'
                if not rerun and not ignore_code_changes:
                    if rec.run_code_id not in run_unchanged:
                        rerun, reason = True, 'run code changed'
                    elif rec.uses_code_id not in uses_unchanged:
                        rerun, reason = True, 'uses= changed'
                    elif rec.io_code_id not in io_unchanged:
                        rerun, reason = True, 'inputs/outputs spec changed'
                if not rerun and check_outputs == 'always' and task.outputs:
                    if not _outputs_complete(task):
                        rerun, reason = True, 'outputs missing (check_outputs=always)'
            # MM: Can we not skip the above logic if forced?
            if force:
                rerun, reason = True, 'forced'

            if not rerun:
                if upstream_all or any(task_kwargs in dep_rerun for dep_rerun in elementwise_deps):
                    rerun, reason = True, 'upstream reruns'
            # Durable cross-pass backstop: an upstream committed in a later
            # invocation than this task (e.g. an upstream rerun via `run -Q`,
            # or after a crash) without rerunning it in the same pass. run_seq
            # None = not-yet-tracked (pre-upgrade): don't rerun on that alone.
            # MM: Can't quite see how task_run_seq works here.
            if not rerun and rec is not None and rec.run_seq is not None:
                up_seq = _max_upstream_run_seq(rule, task_kwargs, task_run_seq)
                if up_seq is not None and up_seq > rec.run_seq:
                    rerun, reason = True, 'upstream ran more recently'

            logger.trace('{}: {} — {}', task.key, 'rerun' if rerun else 'skip', reason)
            if rerun:
                rule_rerun.add(task_kwargs)
                runnable.append(task)

        rerun_kwargs[rule] = rule_rerun
        # MM: oh, it's a dict of all currently know tasks grouped by rule I think.
        task_run_seq[rule] = {
            frozenset(task.kwargs.items()):
                (records[task.key].run_seq if task.key in records else None)
            for task in tasks
        }
        logger.debug('{}: {} task(s), {} to rerun', rule.name, len(tasks), len(rule_rerun))

    logger.debug(
        'plan: {} runnable, {} deferred in {:.3f}s',
        len(runnable), len(deferred), perf_counter() - start,
    )
    return runnable, deferred
