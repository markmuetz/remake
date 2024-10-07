from collections import defaultdict

import networkx as nx
from pyvis.network import Network


def display_remake_rule_graph(remakes):
    input_paths = defaultdict(set)
    output_paths = {}
    for remake in remakes:
        for rule in remake.rules:
            for task in rule.tasks:
                for p in task.inputs.values():
                    input_paths[p].add((remake, task.__class__))
                for p in task.outputs.values():
                    output_paths[p] = (remake, task.__class__)
    g = nx.DiGraph()
    all_edges = set()

    for op, (remake1, rule1) in output_paths.items():
        if op in input_paths:
            rules = input_paths[op]
            for (remake2, rule2) in rules:
                if (rule1.__name__, rule2.__name__) not in all_edges:
                    all_edges.add((rule1.__name__, rule2.__name__))
                    g.add_node(rule1.__name__, group=remake1.name)
                    g.add_node(rule2.__name__, group=remake2.name)
                    g.add_edge(rule1.__name__, rule2.__name__)
    net = Network(
        directed=True,
        notebook=True,
        select_menu=True,
        filter_menu=True,
    )
    # net.show_buttons()
    net.from_nx(g)
    return net.show('ex.html')
