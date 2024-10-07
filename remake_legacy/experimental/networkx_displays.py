import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

from remake.task import RescanFileTask


def files_as_networkx_graph(task_ctrl):
    assert task_ctrl.finalized
    G = nx.DiGraph()
    for task in task_ctrl.tasks:
        for i in task.inputs:
            for o in task.outputs:
                G.add_edge(i, o)
    return G


def display_task_status(task_ctrl):
    pos = {}
    for i, task in enumerate([n for n in task_ctrl.task_dag.nodes if isinstance(n, RescanFileTask)]):
        pos[task] = np.array([-1, i])
    for level, tasks in task_ctrl.tasks_at_level.items():
        for i, task in enumerate(tasks):
            pos[task] = np.array([level, i])

    plt.clf()
    plt.title(task_ctrl.name)
    nx.draw_networkx_nodes(task_ctrl.task_dag, pos, task_ctrl.rescan_tasks, node_color='c')
    nx.draw_networkx_nodes(task_ctrl.task_dag, pos, task_ctrl.completed_tasks, node_color='k')
    nx.draw_networkx_nodes(task_ctrl.task_dag, pos, task_ctrl.running_tasks, node_color='g')
    nx.draw_networkx_nodes(task_ctrl.task_dag, pos, task_ctrl.pending_tasks, node_color='y')
    nx.draw_networkx_nodes(task_ctrl.task_dag, pos, task_ctrl.remaining_tasks, node_color='r')
    nx.draw_networkx_edges(task_ctrl.task_dag, pos)
    plt.pause(0.01)
