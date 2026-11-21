"""Static Graphviz DOT export of the rule or task graph. Emits text only (no
graphviz dependency); runs no tasks and needs no metadata backend."""
from .dag import build_rule_dag, expand_rule
from .deps import ALL, Edges, task_id
from .exceptions import Defer


def _q(s):
    """A DOT double-quoted string: backslash and quote escaped, newlines
    turned into DOT's \\n."""
    s = str(s).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
    return f'"{s}"'


def rule_dot(rmk):
    """DOT for the rule-level DAG (what `rule-dag` prints)."""
    info = rmk.rule_dag()
    lines = ['digraph remake {', '  rankdir=LR;']
    for name in info['order']:
        lines.append(f'  {_q(name)};')
    for name in info['order']:
        for succ in info['edges'][name]:
            lines.append(f'  {_q(name)} -> {_q(succ)};')
    lines.append('}')
    return '\n'.join(lines) + '\n'


def task_dot(rmk):
    """DOT for the task graph: one node per task, edges from the path-derived
    dependency map (core/deps.py). A dependency on ALL upstream tasks draws an
    edge from each of them. A rule whose matrix defers is a single node
    labelled deferred, with a rule-level edge to/from it."""
    dag = build_rule_dag(rmk.rules)
    import networkx as nx

    order = list(nx.topological_sort(dag))
    tasks = {}  # rule -> list[Task] or None (deferred)
    for rule in order:
        try:
            tasks[rule] = expand_rule(rule)
        except Defer:
            tasks[rule] = None

    def node(rule, task=None):
        return f'{rule.name}:{task.key}' if task is not None else rule.name

    lines = ['digraph remake {', '  rankdir=LR;']
    for rule in order:
        lines.append(f'  subgraph {_q("cluster_" + rule.name)} {{')
        lines.append(f'    label={_q(rule.name)};')
        if tasks[rule] is None:
            lines.append(f'    {_q(node(rule))} [label={_q(rule.name + " (deferred)")}, '
                         'style=dashed];')
        else:
            for t in tasks[rule]:
                lines.append(f'    {_q(node(rule, t))} [label={_q(repr(t))}];')
        lines.append('  }')

    edges = Edges(tasks_of=lambda r: tasks[r] if tasks[r] is not None else expand_rule(r))
    seen = set()

    def emit(a, b):
        if (a, b) not in seen:
            seen.add((a, b))
            lines.append(f'  {_q(a)} -> {_q(b)};')

    for rule in order:
        for dep in dag.predecessors(rule):
            if tasks[dep] is None or tasks[rule] is None:
                # Task set unknown on one side: a rule-level edge.
                for a in ([node(dep)] if tasks[dep] is None
                          else [node(dep, t) for t in tasks[dep]]):
                    for b in ([node(rule)] if tasks[rule] is None
                              else [node(rule, t) for t in tasks[rule]]):
                        emit(a, b)
                continue
            by_id = {task_id(t): t for t in tasks[dep]}
            for t in tasks[rule]:
                up = edges.upstream_of(dep, t)
                ups = tasks[dep] if up is ALL else [by_id[i] for i in up if i in by_id]
                for u in ups:
                    emit(node(dep, u), node(rule, t))
    lines.append('}')
    return '\n'.join(lines) + '\n'
