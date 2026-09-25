"""The planner — decides which tasks need running. Pure except for metadata
reads (injected).

Rerun logic is DB-first: never run, failed, rule_run code changed (AST
compare), uses changed, or any relevant upstream task reruns. Filesystem
checks happen only via the opt-in check_outputs modes.
"""
import ast
import builtins
import difflib
import inspect
import os
from collections import namedtuple
from time import perf_counter

import networkx as nx
from loguru import logger

from ..metadata.metadata_manager import TASK_STATUS_FAILED, TASK_STATUS_SUCCESS
from ..util.code_compare import CodeComparer
from .dag import expand_rule
from .deps import ALL, Edges, downstream_task, task_id
from .exceptions import Defer, RemakeError
from .rule import is_deferrable
from .scope import (
    io_hash,
    parse_io_hash,
    parse_uses_hash,
    raw_uses_parts,
    uses_hash,
    uses_parts,
)


# Builtins a query may call (review 2026-09-24 M14): with none, reasonable
# queries like "year in range(2000, 2005)" silently matched nothing.
QUERY_BUILTINS = {
    name: getattr(builtins, name)
    for name in ('abs', 'all', 'any', 'bool', 'float', 'frozenset', 'int', 'len',
                 'list', 'max', 'min', 'range', 'round', 'set', 'sorted', 'str', 'tuple')
}


def rule_kwarg_names(rules):
    """Every matrix kwarg name across `rules` — the rule functions'
    parameters after inputs/outputs (the signature contract makes these the
    matrix keys, for callable matrices too)."""
    names = set()
    for rule in rules:
        names.update(p for p in inspect.signature(rule.fn).parameters
                     if p not in ('inputs', 'outputs'))
    return names


def _free_names(tree):
    """Names a query expression reads, minus those it binds itself
    (comprehension targets, lambda arguments)."""
    loaded, bound = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            (loaded if isinstance(node.ctx, ast.Load) else bound).add(node.id)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
    return loaded - bound


def make_predicate(query, known_names=None):
    """Compile a task-filter expression evaluated against task kwargs plus
    'rule' (the rule name), e.g. "year > 1985 and model == 'era5'",
    "rule in ['extract', 'clean']". A few safe builtins are available
    (QUERY_BUILTINS).

    `known_names` (every kwarg name in the pipeline, see rule_kwarg_names)
    turns a name no rule has — almost always a typo — into an error instead
    of a silent no-match. Bad syntax and evaluation errors are RemakeErrors
    (a clean CLI error) rather than raw tracebacks."""
    # MM: this looks like a risk - using compile to compile the code?
    # See how pyquerylist does this - it only allows certain Python ops.
    try:
        tree = ast.parse(query, mode='eval')
        code = compile(tree, '<query>', 'eval')
    except SyntaxError as e:
        raise RemakeError(f'invalid query {query!r}: {e.msg}') from None
    if known_names is not None:
        unknown = _free_names(tree) - set(known_names) - {'rule'} - QUERY_BUILTINS.keys()
        if unknown:
            raise RemakeError(
                f'query {query!r} uses unknown name(s) {sorted(unknown)} — not a '
                f'matrix key of any rule (keys: {sorted(known_names)}), "rule", or an '
                f'allowed builtin ({", ".join(sorted(QUERY_BUILTINS))})'
            )
    def predicate(kwargs):
        try:
            # kwargs go in the *globals*: a generator expression (e.g. inside
            # any()) has its own scope and can't see eval's locals.
            return bool(eval(code, {**kwargs, '__builtins__': QUERY_BUILTINS}))
        except NameError:
            # Query references a kwarg this rule doesn't have: no match.
            return False
        except Exception as e:
            raise RemakeError(
                f'query {query!r} failed on {dict(kwargs)}: {type(e).__name__}: {e}'
            ) from None

    return predicate


def _upstream_rerunning(rule, rerun_kwargs):
    """Any depends_on upstream rerunning this wave? An entry is 'all'
    (truthy) or a set of kwarg-tuples (truthy when non-empty); a rule that
    fully passes leaves an empty set (falsy)."""
    return any(rerun_kwargs.get(dep) for dep in rule.depends_on)


def _reads_rerun(upstream, dep_rerun):
    """Does a task reading `upstream` (deps.ALL or a set of upstream task
    ids) read any of `dep_rerun` (a non-empty set of rerunning task ids)?"""
    return upstream is ALL or not upstream.isdisjoint(dep_rerun)


class _UpstreamSeqs:
    """run_seq per upstream task — {rule: {task id: run_seq or None}} —
    fetched lazily per rule by `fetch(rule)` (None: not known), with each
    rule's max cached so the backstop's cheap pre-check is O(1) per task."""

    def __init__(self, fetch):
        self._fetch = fetch
        self._cache = {}

    def get(self, rule):
        if rule not in self._cache:
            seqs = self._fetch(rule)
            top = None
            if seqs:
                top = max((s for s in seqs.values() if s is not None), default=None)
            self._cache[rule] = (seqs, top)
        return self._cache[rule]


def _max_upstream_run_seq(task, upstream_seqs, edges, above=None):
    """Highest run_seq among the upstream tasks this task reads (see
    deps.Edge; every upstream task when it reads ALL). Returns None when no
    upstream run_seq is known. `above` short-circuits: an upstream rule whose
    newest task is not above it can't matter, so its edge is never built."""
    best = None
    for dep in task.rule.depends_on:
        seqs, top = upstream_seqs.get(dep)
        if top is None or (above is not None and top <= above):
            continue
        upstream = edges.upstream_of(dep, task)
        candidates = seqs.values() if upstream is ALL else (seqs.get(u) for u in upstream)
        for s in candidates:
            if s is not None and (best is None or s > best):
                best = s
    return best


def _outputs_complete(task):
    outputs = task.outputs
    return bool(outputs) and all(token.is_complete() for token in outputs.values())


def cascade_settled(rule_set, dag, selected, run_seq, status, edges=None):
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

    `edges` (deps.Edges) says which upstream tasks each task reads; built
    from the rules' matrices if not given.
    """
    edges = edges or Edges()
    settled = {rule: set(ids) for rule, ids in selected.items()}
    for rule in nx.topological_sort(dag):
        if rule not in rule_set or not rule.depends_on:
            continue
        for task_kwargs, st in status.get(rule, {}).items():
            if st != TASK_STATUS_SUCCESS or task_kwargs in settled.get(rule, set()):
                continue
            this_seq = run_seq.get(rule, {}).get(task_kwargs)
            downstream_of_settled = independent_newer = False
            task = downstream_task(rule, task_kwargs)
            for dep in rule.depends_on:
                upstream = edges.upstream_of(dep, task)
                dep_ids = list(run_seq.get(dep, {})) if upstream is ALL else upstream
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


# Key in an executor's `failures` dict holding the normalised output paths of
# every task that failed (or was skipped) this run.
_FAILED_OUTPUTS = '_failed_outputs'


def _norm_paths(values):
    return {os.path.normpath(str(v)) for v in values}


def _io_paths(task, part):
    """A task's normalised input or output paths, or an empty set if its
    callable spec raises — run_task has already recorded that failure, and
    the executor's bookkeeping must not re-raise it (and crash the run)."""
    try:
        return _norm_paths(getattr(task, part).values())
    except Exception:
        return set()


def record_failure(failures, task):
    """Record a failed (or skipped-for-upstream-failure) task in an
    executor's `failures` dict, for upstream_failed."""
    failures.setdefault(task.rule, set()).add(frozenset(task.kwargs.items()))
    failures.setdefault(_FAILED_OUTPUTS, set()).update(_io_paths(task, 'outputs'))


def upstream_failed(task, failures, edges):
    """Should task be skipped because upstream tasks failed this run?

    failures: built by record_failure; edges: a deps.Edges for the run.
    Mirrors the planner's rerun propagation: skipped when the task reads a
    failed upstream task's outputs, or reads ALL of a rule with failures
    (ordering-only or shared outputs). The direct path check also catches a
    failed task's outputs read across rules the edge map doesn't cover.
    """
    if _io_paths(task, 'inputs') & failures.get(_FAILED_OUTPUTS, set()):
        return True
    for dep in task.rule.depends_on:
        failed = failures.get(dep)
        if failed and _reads_rerun(edges.upstream_of(dep, task), failed):
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


def _task_label(task):
    kstr = ', '.join(f'{k}={v}' for k, v in task.kwargs.items())
    return f'{task.rule.name}[{kstr}]'


def _fetch_run_seqs(metadata, rule, tasks=None):
    """{task id: run_seq or None} for all of `rule`'s tasks (None if its
    matrix defers)."""
    if tasks is None:
        try:
            tasks = expand_rule(rule)
        except Defer:
            return None
    recs = metadata.get_tasks_status(tasks)
    return {task_id(t): (recs[t.key].run_seq if t.key in recs else None) for t in tasks}


def upstream_run_seqs(metadata):
    """Per-rule run_seqs fetched lazily from `metadata` — share one across
    a batch of explain_task calls."""
    return _UpstreamSeqs(lambda dep: _fetch_run_seqs(metadata, dep))


def explain_task(rules, dag, metadata, task, *, check_outputs='never', runnable=None,
                 edges=None, upstream_seqs=None):
    """Why would (or wouldn't) this task run? Returns (will_run, reasons),
    each reason a `Reason(category, message)` in the order the planner checks
    them. The `remake why` command (messages) and `info --reasons` (categories).

    `runnable` is the precomputed `plan()` runnable list; pass it to explain
    many tasks without re-planning per task (one plan() shared across them).
    Computed internally when not supplied (the single-task case). Likewise
    `edges` (deps.Edges) and `upstream_seqs` (upstream_run_seqs): without
    them each call re-resolves the upstream paths and refetches run_seqs."""
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
        stored_io = codes.get(rec.io_code_id)
        current_io = io_hash(task.rule)
        if rec.io_code_id is not None and stored_io != current_io:
            # Name which segment differs — diagnosing an io-changed rerun
            # previously meant pulling the stored string from the DB and
            # diffing its segments by hand (todos.md, wescon 2026-06-17).
            old_segs, new_segs = parse_io_hash(stored_io or ''), parse_io_hash(current_io)
            changed = [part for part in ('inputs', 'outputs')
                       if old_segs[part] != new_segs[part]] or ['inputs', 'outputs']
            reasons.append(Reason('io-changed',
                f'inputs/outputs spec changed since last run: '
                f'{" and ".join(changed)} segment(s) differ'))
        if check_outputs == 'always' and task.outputs and not _outputs_complete(task):
            reasons.append(Reason('outputs-missing',
                'outputs missing/incomplete (check_outputs=always)'))

    in_pass_upstream = False
    edges = edges or Edges()
    for dep in task.rule.depends_on:
        dep_running = [t for t in runnable if t.rule is dep]
        if not dep_running:
            continue
        upstream = edges.upstream_of(dep, task)
        if upstream is ALL:
            in_pass_upstream = True
            reasons.append(Reason('upstream-rerun',
                f'{len(dep_running)} upstream {dep.name} task(s) rerun (this task reads '
                f'no output of {dep.name} that only one task writes: depends on all of it)'))
        else:
            match = [t for t in dep_running if task_id(t) in upstream]
            if match:
                in_pass_upstream = True
                names = ', '.join(_task_label(t) for t in match[:3])
                more = f' (+{len(match) - 3} more)' if len(match) > 3 else ''
                reasons.append(Reason('upstream-rerun',
                    f'upstream {names}{more} rerun{"s" if len(match) == 1 else ""} '
                    f'(this task reads {"its" if len(match) == 1 else "their"} outputs)'))

    # Durable cross-pass propagation: an upstream was committed in a later
    # invocation than this task without rerunning it in the same pass (the gap
    # that an in-pass-only signal misses). Only reported when nothing upstream
    # is rerunning *this* pass — otherwise the upstream-rerun reason above is
    # the live cause. Mirrors the planner's `_max_upstream_run_seq` check.
    if rec is not None and rec.run_seq is not None and not in_pass_upstream:
        up_seq = _max_upstream_run_seq(
            task, upstream_seqs or upstream_run_seqs(metadata), edges)
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
    predicate = make_predicate(query, rule_kwarg_names(rules)) if query else None
    code_comparer = CodeComparer()
    rules = set(rules)
    nmatched = 0

    runnable = []
    deferred = []
    rerun_kwargs = {}  # rule -> set of frozenset(kwargs.items()), or 'all'
    # rule -> {frozenset(kwargs.items()): run_seq or None}. Threaded in topo
    # order so a task can compare its stored run_seq against its upstreams'
    # (durable cross-pass propagation; see bugs/01_durable_rerun_propagation.md).
    task_run_seq = {}
    planned = {}  # rule -> its expanded tasks this plan

    # Under a query, an upstream task a selected task reads may itself be
    # filtered out: the edge map and the backstop see the upstream's full
    # task set (one extra expansion + status fetch per upstream rule, paid
    # only when an edge or the backstop needs it).
    full_tasks = {}

    def tasks_of(dep):
        if predicate is None and dep in planned:
            return planned[dep]
        if dep not in full_tasks:
            full_tasks[dep] = expand_rule(dep)
        return full_tasks[dep]

    def fetch_run_seqs(dep):
        if dep not in task_run_seq:
            return None  # deferred, or outside `rules`
        if predicate is None:
            return task_run_seq[dep]
        return _fetch_run_seqs(metadata, dep, tasks_of(dep))

    edges = Edges(tasks_of)
    upstream_seqs = _UpstreamSeqs(fetch_run_seqs)

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
        rule_rerun = set()

        # Records carry code *ids*, not text; resolve the distinct few (per
        # rule, typically 1 of each — more only when tasks last ran under
        # different code versions) and compare each against the current state
        # once. The per-task check below is then set membership on ints —
        # this is what keeps status+plan cost from scaling with task count
        # (logs_analysis §1.1/1.2). Skipped entirely when nothing will read
        # the sets: force reruns unconditionally, ignore_code_changes skips
        # the freshness checks — either way the source rendering and compares
        # would be pure waste (uses_hash alone can render ~100 KB per rule).
        run_unchanged = uses_unchanged = io_unchanged = frozenset()
        if not force and not ignore_code_changes:
            run_src = rule.source['run']
            current_uses_hash = uses_hash(rule.uses)
            current_io_hash = io_hash(rule)
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

        # Same-pass propagation inputs: an upstream rerunning 'all' (deferred
        # or unknown task set) reruns every task here; one rerunning some
        # tasks reruns the tasks that read them (deps.Edge, per task below).
        upstream_all = False
        partial_deps = []
        for dep in rule.depends_on:
            dep_rerun = rerun_kwargs.get(dep, set())
            if dep_rerun == 'all':
                upstream_all = True
            elif dep_rerun:
                partial_deps.append((dep, dep_rerun))

        for task in tasks:
            rec = records.get(task.key)
            task_kwargs = frozenset(task.kwargs.items())
            # `reason` is a short literal (cheap to assign every iteration);
            # only formatted into a log line when a TRACE sink is attached.
            if force:
                # Unconditional: skip the freshness checks (and their stat
                # calls under check_outputs) rather than compute-then-discard.
                rerun, reason = True, 'forced'
            elif rec is None:
                if check_outputs in ('fallback', 'always') and _outputs_complete(task):
                    rerun, reason = False, 'outputs complete (no DB record)'
                else:
                    rerun, reason = True, 'never run (no DB record)'
            else:
                rerun = rec.status != TASK_STATUS_SUCCESS
                reason = 'last run not successful' if rerun else 'up to date'
                if not rerun and not ignore_code_changes:
                    # All three are cheap int set-memberships, so record every
                    # trigger rather than the first (several can be true at
                    # once). The later checks (stat calls, upstream scan) stay
                    # short-circuited on purpose — they cost real work, and
                    # explain_task is the full-fidelity view.
                    changed = []
                    if rec.run_code_id not in run_unchanged:
                        changed.append('run code changed')
                    if rec.uses_code_id not in uses_unchanged:
                        changed.append('uses= changed')
                    if rec.io_code_id not in io_unchanged:
                        changed.append('inputs/outputs spec changed')
                    if changed:
                        rerun, reason = True, ' + '.join(changed)
                if not rerun and check_outputs == 'always' and task.outputs:
                    if not _outputs_complete(task):
                        rerun, reason = True, 'outputs missing (check_outputs=always)'

            if not rerun:
                if upstream_all or any(_reads_rerun(edges.upstream_of(dep, task), dep_rerun)
                                       for dep, dep_rerun in partial_deps):
                    rerun, reason = True, 'upstream reruns'
            # Durable cross-pass backstop: an upstream committed in a later
            # invocation than this task (e.g. an upstream rerun via `run -Q`,
            # or after a crash) without rerunning it in the same pass. run_seq
            # None = not-yet-tracked (pre-upgrade): don't rerun on that alone.
            # task_run_seq holds every planned task's stored run_seq, by rule
            # (filled in below as each rule is planned, in topo order).
            if not rerun and rec is not None and rec.run_seq is not None:
                up_seq = _max_upstream_run_seq(task, upstream_seqs, edges, above=rec.run_seq)
                if up_seq is not None and up_seq > rec.run_seq:
                    rerun, reason = True, 'upstream ran more recently'

            logger.trace('{}: {} — {}', task.key, 'rerun' if rerun else 'skip', reason)
            if rerun:
                rule_rerun.add(task_kwargs)
                runnable.append(task)

        rerun_kwargs[rule] = rule_rerun
        planned[rule] = tasks
        task_run_seq[rule] = {
            frozenset(task.kwargs.items()):
                (records[task.key].run_seq if task.key in records else None)
            for task in tasks
        }
        nmatched += len(tasks)
        logger.debug('{}: {} task(s), {} to rerun', rule.name, len(tasks), len(rule_rerun))

    if predicate is not None and not nmatched and not deferred:
        # Distinguish "your query selected nothing" from "all up to date".
        logger.warning(f'query {query!r} matched no tasks')
    elapsed = perf_counter() - start
    logger.bind(
        event='plan', nrunnable=len(runnable), ndeferred=len(deferred),
        seconds=round(elapsed, 6),
    ).debug(
        'plan: {} runnable, {} deferred in {:.3f}s',
        len(runnable), len(deferred), elapsed,
    )
    return runnable, deferred
