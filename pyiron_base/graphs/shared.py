from concurrent.futures import Future
from time import sleep

from IPython.display import SVG, display
import matplotlib.pyplot as plt
import networkx as nx


def draw(graph):
    svg = nx.nx_agraph.to_agraph(graph).draw(prog="dot", format="svg")
    display(SVG(svg))
    plt.show()


def add_node(graph, key, value, link_to, link_to_label=None, show_values=False):
    node_name = hash(str(value))  # dangerous because it is not unique
    if key is not None:
        if show_values:
            graph.add_node(node_name, label=str(key), color="green")
            if link_to_label is not None:
                graph.add_edge(node_name, link_to, label=link_to_label)
            else:
                graph.add_edge(node_name, link_to, label=str(key) + ": " + str(value))
        else:
            graph.add_node(node_name, label=str(key), color="green")
            if link_to_label is not None:
                graph.add_edge(node_name, link_to, label=link_to_label)
            else:
                graph.add_edge(node_name, link_to)
    else:
        # This case is triggered by the GraphSplitList
        graph.add_node(node_name, label=str(value), color="green")
        if link_to_label is not None:
            graph.add_edge(node_name, link_to, label=link_to_label)
        else:
            graph.add_edge(node_name, link_to)


def execute_workflow(workflow, sleep_period=0.01):
    while not (workflow.future is not None and workflow.future.done()):
        sleep(sleep_period)
        task_lst = workflow.get_tasks()
        for graph, funct, kwargs, executor in task_lst:
            if executor is not None:
                graph.future = executor.submit(funct, **kwargs)
            else:
                graph.future = Future()
                graph.future.set_result(funct(**kwargs))
        if len(task_lst) == 1 and task_lst[0][0] == workflow:
            break
