"""Task-to-task dependencies across one `depends_on` edge, derived from paths.

Which upstream tasks a downstream task depends on is a property of its
resolved *inputs*, not of its matrix: equal matrices say two rules have the
same task set, nothing about which upstream output each task reads (a task
may read upstream `year-1`, or a stencil, or every element). Review
2026-09-24 H3; design in design_docs/code_reviews/2026-09-24_review.md,
Appendix A.

One implementation serves the planner (rerun propagation and the durable
run_seq backstop), `set-state`'s cascade, the executors' failure-skip and
SLURM's aftercorr/afterok choice.

The map is only as good as the declared inputs: a task that reads upstream
files it does not declare (e.g. by globbing) is invisible to it. A task
whose inputs hit no upstream output at all (an ordering-only `depends_on`),
or hit an output several upstream tasks share, depends on ALL of them.
"""
import os
import string

from .dag import expand_rule
from .exceptions import Defer
from .rule import _template_fields
from .task import Task
from .tokens import OutputToken

# Can't tell precisely: the task depends on every upstream task.
ALL = 'all'

_ELEMENTWISE, _ORDERING, _RESOLVE = 'elementwise', 'ordering', 'resolve'


def task_id(task):
    return frozenset(task.kwargs.items())


def _norm(value):
    return os.path.normpath(str(value))


def _templates(spec):
    """Template strings of a dict inputs/outputs spec; None for a callable
    spec (its paths are only known by resolving it)."""
    if spec is None:
        return []
    if callable(spec) and not isinstance(spec, dict):
        return None
    return [v.identity() if isinstance(v, OutputToken) else str(v) for v in spec.values()]


def _literal_ends(template):
    """(prefix, suffix): the literal text before the first and after the last
    format field (the whole string for both if it has none)."""
    parsed = list(string.Formatter().parse(template))
    if not any(field is not None for _, field, _, _ in parsed):
        return template, template
    return parsed[0][0], template[template.rindex('}') + 1:]


def _clean(literal):
    """A literal fragment normpath can't rewrite: no '//' and no '.'/'..'
    path segments (so comparing it with the resolved, normpath'd path is
    sound)."""
    return '//' not in literal and not any(seg in ('.', '..') for seg in literal.split('/'))


def _disjoint(a, b):
    """Provably no path is produced by both templates: their literal
    prefixes (or suffixes) disagree. False means "can't tell"."""
    (pa, sa), (pb, sb) = _literal_ends(a), _literal_ends(b)
    # The trailing partial segment of a prefix (and leading of a suffix)
    # continues into a field value, so only its full segments must be clean.
    if (_clean(pa.rsplit('/', 1)[0]) and _clean(pb.rsplit('/', 1)[0])
            and not (pa.startswith(pb) or pb.startswith(pa))):
        return True
    sa, sb = sa.rstrip('/'), sb.rstrip('/')
    return (_clean(sa.split('/', 1)[-1]) and _clean(sb.split('/', 1)[-1])
            and not (sa.endswith(sb) or sb.endswith(sa)))


def _classify(dep, rule):
    """Decide an edge from its templates alone where that is sound, so the
    common shapes never resolve a path (cost: ~2 s per 1e6 paths).

    - every input template provably disjoint from every upstream output
      template: ordering-only, each task depends on ALL;
    - the inputs=upstream.outputs idiom (or any input template equal to an
      upstream output template naming every matrix key), shared matrix, all
      other input templates disjoint: element-wise;
    - anything else (callable specs, format escapes, templates that might
      overlap): resolve paths.
    Assumes, as declared templates imply, that distinct matrix kwargs format
    to distinct paths and that no two upstream templates write the same file;
    and that matrix values contain no '/', '.' or '..' path segments (a value
    like 'x/..' can make literally disjoint templates collide after normpath
    — documented in docs/guide/rules-and-tasks.md, not checked).
    """
    ins, outs = _templates(rule.inputs), _templates(dep.outputs)
    if ins is None or outs is None or any('{{' in t or '}}' in t for t in ins + outs):
        return _RESOLVE
    out_set = set(outs)
    equal = [t for t in ins if t in out_set]
    if any(not _disjoint(t, o) for t in ins if t not in out_set for o in outs):
        return _RESOLVE
    if not equal:
        return _ORDERING
    same_matrix = rule.matrix is dep.matrix or rule.matrix == dep.matrix
    if same_matrix and dep.matrix is not None and not callable(dep.matrix):
        if isinstance(dep.matrix, list):
            keys = set(dep.matrix[0]) if dep.matrix else set()
        else:  # dict form; tuple keys bind several kwargs
            keys = {k for key in dep.matrix
                    for k in (key if isinstance(key, tuple) else (key,))}
        if all(keys <= _template_fields(t) for t in equal):
            return _ELEMENTWISE
    elif same_matrix and dep.matrix is None:
        return _ELEMENTWISE  # one task each side
    return _RESOLVE


class Edge:
    """The upstream tasks each downstream task of `rule` reads from `dep`."""

    def __init__(self, dep, rule, dep_tasks):
        self.dep, self.rule = dep, rule
        self._dep_tasks = dep_tasks  # zero-arg callable, called at most once
        self._kind = _classify(dep, rule)
        self._producer = None
        self._shared = None

    def _index(self):
        if self._producer is None:
            producer, shared = {}, set()
            try:
                dep_tasks = self._dep_tasks()
            except Defer:
                dep_tasks = None  # upstream task set unknown yet
            if dep_tasks is None:
                self._kind = _ORDERING
            else:
                for t in dep_tasks:
                    tid = task_id(t)
                    try:
                        outputs = t.outputs.values()
                    except Exception:
                        continue  # its own resolution error is reported elsewhere
                    for tok in outputs:
                        p = _norm(tok)
                        if producer.get(p, tid) != tid:
                            shared.add(p)
                        producer[p] = tid
            self._producer, self._shared = producer, shared
        return self._producer

    def upstream_of(self, task):
        """ALL, or the frozenset of upstream task ids (frozenset of kwargs
        items) whose outputs `task` reads."""
        if self._kind == _ELEMENTWISE:
            return frozenset([task_id(task)])
        if self._kind == _RESOLVE:
            producer = self._index()
        if self._kind == _ORDERING:
            return ALL
        try:
            paths = {_norm(p) for p in task.inputs.values()}
        except Exception:
            return ALL  # the task fails on its own spec; stay conservative
        hits = paths & producer.keys()
        if not hits or hits & self._shared:
            return ALL
        return frozenset(producer[p] for p in hits)

    @property
    def elementwise_by_template(self):
        return self._kind == _ELEMENTWISE


class Edges:
    """Lazily built Edge per (dep, rule), cached for one plan/run.

    `tasks_of(rule)` returns the rule's full task list (default: expand its
    matrix; may raise Defer)."""

    def __init__(self, tasks_of=None):
        self._tasks_of = tasks_of or expand_rule
        self._edges = {}

    def get(self, dep, rule):
        key = (dep, rule)
        if key not in self._edges:
            self._edges[key] = Edge(dep, rule, lambda: self._tasks_of(dep))
        return self._edges[key]

    def upstream_of(self, dep, task):
        return self.get(dep, task.rule).upstream_of(task)


def downstream_task(rule, tid):
    """A Task for `rule` from its task id (for callers holding only ids)."""
    return Task(rule=rule, kwargs=dict(tid))
