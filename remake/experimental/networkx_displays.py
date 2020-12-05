import networkx as nx
import numpy as np


def tasks_as_networkx_graph(task_ctrl):
    assert task_ctrl.finalized
    G = nx.DiGraph()
    for task in task_ctrl.tasks:
        G.add_node(task)

        for prev_task in task_ctrl.prev_tasks[task]:
            G.add_edge(prev_task, task)
    return G


def files_as_networkx_graph(task_ctrl):
    assert task_ctrl.finalized
    G = nx.DiGraph()
    for task in task_ctrl.tasks:
        for i in task.inputs:
            for o in task.outputs:
                G.add_edge(i, o)
    return G


def display_task_status(task_ctrl):
    import matplotlib.pyplot as plt
    TG = tasks_as_networkx_graph(task_ctrl)
    pos = {}
    for level, tasks in task_ctrl.tasks_at_level.items():
        for i, task in enumerate(tasks):
            pos[task] = np.array([level, i])

    plt.clf()
    nx.draw_networkx_nodes(TG, pos, task_ctrl.completed_tasks, node_color='k')
    nx.draw_networkx_nodes(TG, pos, task_ctrl.running_tasks, node_color='g')
    nx.draw_networkx_nodes(TG, pos, task_ctrl.pending_tasks, node_color='y')
    nx.draw_networkx_nodes(TG, pos, task_ctrl.remaining_tasks, node_color='r')
    nx.draw_networkx_edges(TG, pos)
    plt.pause(0.01)
