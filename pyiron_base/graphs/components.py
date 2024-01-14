from concurrent.futures import Future
from inspect import signature
from threading import Thread

import matplotlib.pyplot as plt  # This is required otherwise pympipool fails
import networkx as nx

from pyiron_base.graphs.shared import add_node, draw, execute_workflow


class FunctionWrapper:
    def __init__(self, funct, executor):
        self._funct = funct
        self._name = funct.__name__
        self.__signature__ = signature(funct)
        self._executor = executor

    @property
    def name(self):
        return self._name

    def __call__(self, *args, **kwargs):
        return GraphFunction(
            funct=self._funct, executor=self._executor, *args, **kwargs
        )


class GraphFunction(FunctionWrapper):
    def __init__(self, funct, executor, *args, **kwargs):
        super().__init__(funct=funct, executor=executor)
        self._links = self.__signature__.bind(*args, **kwargs)
        self._future = None
        self._process = None
        self._executor = executor

    @property
    def future(self):
        return self._future

    @future.setter
    def future(self, future_obj):
        self._future = future_obj

    def get_status(self):
        status_lst = self._get_status_of_links().values()
        if self.future is not None and self.future.done():
            return "done"
        elif self.future is not None:
            return "running"
        elif (
            "done" in status_lst
            and "wait" not in status_lst
            and "running" not in status_lst
            and "ready" not in status_lst
        ):
            return "ready"
        else:
            return "wait"

    def ready(self):
        if self.future is not None and self.future.done():
            return True
        else:
            return False

    def get_links(self):
        return self._links.arguments.items()

    def get_graph(self):
        return get_graph(workflow=self)

    def get_tasks(self):
        kwargs = {}
        task_lst = []
        if self.ready():
            return task_lst
        elif self.future is not None:
            return []
        else:
            return_task_lst = False
            for key, value in self.get_links():
                if (
                    isinstance(value, (GraphFunction, GraphGather))
                    and value.future is None
                ):
                    task_lst += value.get_tasks()
                elif (
                    isinstance(value, GraphFunction) and value.future.done()
                ) or isinstance(value, (GraphGather, GraphItem, GraphScatter)):
                    kwargs[key] = value.result()
                elif not isinstance(
                    value, (GraphFunction, GraphGather, GraphItem, GraphScatter)
                ):
                    kwargs[key] = value
                else:
                    return_task_lst = True
            if len(task_lst) > 0 or return_task_lst:
                return task_lst
            else:
                if self.get_status() == "ready":
                    return [[self, self._funct, kwargs, self._executor]]
                else:
                    return []

    def result(self):
        if self.future is None:
            kwargs = {
                key: (
                    value.result()
                    if isinstance(
                        value,
                        (GraphFunction, GraphGather, GraphItem, GraphScatter),
                    )
                    else value
                )
                for key, value in self.get_links()
            }
            self._future = Future()
            self.future.set_result(self._funct(**kwargs))
        return self.future.result()

    def draw_recursive(
        self, graph, link_to=None, link_to_label=None, show_values=False
    ):
        draw_recursive_with_label(
            node_name=self._name + "_" + str(self.__hash__()),
            graph=graph,
            label=self._name,
            links=self.get_links(),
            ready=self.ready(),
            link_to=link_to,
            link_to_label=link_to_label,
            show_values=show_values,
        )

    def draw(self, show_values=False):
        graph = nx.DiGraph()
        self.draw_recursive(
            graph=graph, link_to=None, link_to_label=None, show_values=show_values
        )
        return draw(graph=graph)

    def run(self):
        execute_workflow(workflow=self, sleep_period=0.01)

    def run_in_background(self):
        self._process = Thread(target=execute_workflow, kwargs={"workflow": self})
        self._process.start()

    def _get_status_of_links(self):
        return {
            key: (
                value.get_status()
                if isinstance(value, (GraphFunction, GraphGather))
                else "done"
            )
            for key, value in self.get_links()
        }

    def __getattr__(self, name):
        if name in self._links.arguments.keys():
            return self._links.arguments[name]
        else:
            raise AttributeError(name)


class GraphScatter:
    def __init__(self, lst):
        self._lst_raw = lst
        self._lst = [GraphItem(item=item, link=self) for item in lst]
        self._ind = 0

    def __iter__(self):  # This has to be updated to include linked objects
        return iter(self._lst)

    def __next__(self):
        self._ind += 1
        if self._ind < len(self._lst):
            return self._lst[self._ind - 1]
        raise StopIteration

    def __dir__(self):
        return dir(self._lst)

    def __str__(self):
        return str(self._lst)

    def ready(self):
        return True

    def get_links(self):
        return {"list": self._lst_raw}.items()

    def draw_recursive(
        self, graph, link_to=None, link_to_label=None, show_values=False
    ):
        draw_recursive_with_label(
            node_name="scatter_" + str(hash(str(self._lst_raw))),
            graph=graph,
            label="scatter",
            links=self.get_links(),
            ready=self.ready(),
            link_to=link_to,
            link_to_label=link_to_label,
            show_values=show_values,
        )


class GraphItem:
    def __init__(self, item, link):
        self._item = item
        self._link = link

    def __dir__(self):
        return dir(self._item)

    def __str__(self):
        return str(self._item)

    def ready(self):
        return True

    def get_links(self):
        return {self._item: self._link}.items()

    def draw_recursive(
        self, graph, link_to=None, link_to_label=None, show_values=False
    ):
        if show_values:
            link_to_label += ": " + str(self)
        draw_recursive_without_label(
            graph=graph,
            links=self.get_links(),
            link_to=link_to,
            link_to_label=link_to_label,
            show_values=show_values,
        )

    def result(self):
        return self._item


class GraphGather:
    def __init__(self, lst):
        self._links = {i: item for i, item in enumerate(lst)}
        self._lst_raw = lst
        self._future = None

    @property
    def future(self):
        return self._future

    @future.setter
    def future(self, future_obj):
        self._future = future_obj

    def get_links(self):
        return self._links.items()

    def get_tasks(self):
        task_lst, node_lst, link_lst = [], [], []
        if self.future is None:
            for item in self._links.values():
                link_lst.append(item)
                for task in item.get_tasks():
                    if task[0] not in node_lst:
                        node_lst.append(task[0])
                        task_lst.append(task)
            if all([node in link_lst for node in node_lst]):
                self._future = Future()
        return task_lst

    def result(self):
        if self.future is None:
            self._future = Future()
        if not self.future.done():
            self.future.set_result([item.result() for item in self._links.values()])
        return self.future.result()

    def get_status(self):
        status_lst = self._get_status_of_links().values()
        if self.future is not None and self.future.done():
            return "done"
        elif self.future is not None:
            return "running"
        elif (
            "done" in status_lst
            and "wait" not in status_lst
            and "running" not in status_lst
            and "ready" not in status_lst
        ):
            return "ready"
        else:
            return "wait"

    def _get_status_of_links(self):
        return {
            key: (
                value.get_status()
                if isinstance(
                    value, (GraphFunction, GraphGather, GraphItem, GraphScatter)
                )
                else "done"
            )
            for key, value in self.get_links()
        }

    def ready(self):
        if self.future is not None and self.future.done():
            return True
        else:
            return False

    def draw_recursive(
        self, graph, link_to=None, link_to_label=None, show_values=False
    ):
        draw_recursive_with_label(
            node_name="gather_" + str(hash(str(self._lst_raw))),
            graph=graph,
            label="gather",
            links=self.get_links(),
            ready=self.ready(),
            link_to=link_to,
            link_to_label=link_to_label,
            show_values=show_values,
        )

    def draw(self, show_values=False):
        graph = nx.DiGraph()
        self.draw_recursive(
            graph=graph, link_to=None, link_to_label=None, show_values=show_values
        )
        return draw(graph=graph)

    def __iter__(self):
        return iter(self._links.values())


def draw_recursive_with_label(
    node_name,
    graph,
    links,
    label=None,
    ready=False,
    link_to=None,
    link_to_label=None,
    show_values=False,
):
    if ready:
        graph.add_node(node_name, label=label, color="green")
    else:
        graph.add_node(node_name, label=label)
    if link_to is not None:
        if link_to_label is not None:
            graph.add_edge(node_name, link_to, label=link_to_label)
        else:
            graph.add_edge(node_name, link_to)
    for key, value in links:
        if isinstance(value, (GraphFunction, GraphGather, GraphItem, GraphScatter)):
            if show_values:
                if (
                    isinstance(value, (GraphFunction, GraphGather))
                    and value.future is not None
                    and value.future.done()
                ):
                    value.draw_recursive(
                        graph=graph,
                        link_to=node_name,
                        link_to_label=str(key) + ": " + str(value.result()),
                        show_values=show_values,
                    )
                else:
                    value.draw_recursive(
                        graph=graph,
                        link_to=node_name,
                        link_to_label=str(key),
                        show_values=show_values,
                    )
            else:
                value.draw_recursive(
                    graph=graph, link_to=node_name, show_values=show_values
                )
        else:
            add_node(
                graph=graph,
                key=key,
                value=value,
                link_to=node_name,
                show_values=show_values,
            )


def draw_recursive_without_label(
    graph, links, link_to=None, link_to_label=None, show_values=False
):
    for key, value in links:
        if isinstance(value, (GraphFunction, GraphGather, GraphItem, GraphScatter)):
            value.draw_recursive(
                graph=graph,
                link_to=link_to,
                link_to_label=link_to_label,
                show_values=show_values,
            )
        else:
            add_node(
                graph=graph,
                key=None,
                value=value,
                link_to=link_to,
                link_to_label=link_to_label,
                show_values=show_values,
            )


def get_graph(workflow):
    return {
        "type": "graphdict",
        "name": workflow.name + "_" + str(workflow.__hash__()),
        "status": workflow.get_status(),
        "links": {
            key: (
                get_graph(workflow=value) if isinstance(value, GraphFunction) else value
            )
            for key, value in workflow.get_links()
        },
    }


def wrap(executor=None):
    def decorator(funct):
        graph_funct = FunctionWrapper(funct=funct, executor=executor)
        graph_funct.__doc__ = funct.__doc__
        return graph_funct

    return decorator
