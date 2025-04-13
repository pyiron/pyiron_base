import hashlib
import inspect
import re
from typing import Dict, List, Optional, Tuple

import cloudpickle

from pyiron_base.jobs.job.extension.server.generic import Server


def draw(node_dict: Dict[str, object], edge_lst: List[List[str]]) -> None:
    """
    Draw graph of nodes and edges

    Args:
        node_dict (Dict[str, object]): Dictionary of nodes
        edge_lst (List[List[str]]): List of edges
    """
    import networkx as nx
    from IPython.display import SVG, display

    graph = nx.DiGraph()
    for k, v in node_dict.items():
        graph.add_node(k, label=str(k).rsplit("_", 1)[0] + "=" + str(v))
    for edge in edge_lst:
        graph.add_edge(edge[1], edge[0])
    svg = nx.nx_agraph.to_agraph(graph).draw(prog="dot", format="svg")
    display(SVG(svg))


def evaluate_function(funct: callable, input_dict: dict) -> object:
    """
    Evaluate python function by using the input dictionary as *args and **kwargs

    Args:
        funct (callable): the function to evaluate
        input_dict (dict): input dictionary

    Returns:
        object: output of the evaluated function
    """
    input_dict = recursive_dict_resolve(input_dict=input_dict)
    if "kwargs" in input_dict.keys():
        input_dict.update(input_dict.pop("kwargs"))
    if "args" in input_dict.keys():
        args = input_dict.pop("args")
        return funct(*args, **input_dict)
    else:
        return funct(**input_dict)


def get_function_parameter_dict(funct: callable) -> dict:
    """
    Get dictionary of parameters for a function

    Args:
        funct (callable): the function to get the parameters for

    Returns:
        dict: parameters for the function
    """
    return {
        k: None if v.default == inspect._empty else v.default
        for k, v in inspect.signature(funct).parameters.items()
    }


def get_graph(
    obj: object,
    obj_name: Optional[str] = None,
    nodes_dict: dict = {},
    edges_lst: list = [],
    link_node: Optional[str] = None,
) -> Tuple[dict, list]:
    """
    Get dictionary of nodes with node names as keys and node objects as values. In addition, generate a list of edges,
    consisting of pairs of node names which are linked together.

    Args:
        obj (object): Object to generate dictionary of nodes and list of edges
        obj_name (str): Name of the object
        nodes_dict (dict): Dictionary of nodes
        edges_lst (list): List of edges
        link_node (str): Name of the node to link to

    Returns:
        dict, list: dictionary of nodes and list of edges
    """
    node_name = get_node_name(node=obj, node_name=obj_name)
    nodes_dict.update({node_name: obj})
    if link_node is not None:
        edges_lst.append([link_node, node_name])
    if isinstance(obj, DelayedObject):
        for k, v in obj._input.items():
            if k == "kwargs":
                for sk, sv in v.items():
                    nodes_dict, edges_lst = get_graph(
                        obj=sv,
                        obj_name=sk,
                        nodes_dict=nodes_dict,
                        edges_lst=edges_lst,
                        link_node=node_name,
                    )
            else:
                nodes_dict, edges_lst = get_graph(
                    obj=v,
                    obj_name=k,
                    nodes_dict=nodes_dict,
                    edges_lst=edges_lst,
                    link_node=node_name,
                )
    elif isinstance(obj, dict) and any(
        [isinstance(v, DelayedObject) for v in obj.values()]
    ):
        for k, v in obj.items():
            nodes_dict, edges_lst = get_graph(
                obj=v,
                obj_name=k,
                nodes_dict=nodes_dict,
                edges_lst=edges_lst,
                link_node=node_name,
            )
    elif isinstance(obj, list) and any([isinstance(v, DelayedObject) for v in obj]):
        for k, v in enumerate(obj):
            nodes_dict, edges_lst = get_graph(
                obj=v,
                obj_name=str(k),
                nodes_dict=nodes_dict,
                edges_lst=edges_lst,
                link_node=node_name,
            )
    return nodes_dict, edges_lst


def get_hash(binary: bytes) -> str:
    """
    Get the hash of a binary string - remove the specification of jupyter kernel from hash to be deterministic

    Args:
        binary (bytes): binary string to hash

    Returns:
        str: hash of the binary string
    """
    binary_no_ipykernel = re.sub(b"(?<=/ipykernel_)(.*)(?=/)", b"", binary)
    return str(hashlib.md5(binary_no_ipykernel).hexdigest())


def get_node_name(node: object, node_name: Optional[str] = None) -> str:
    """
    Get name of the node

    Args:
        node (object): Node to get the name for
        node_name (str): Name of the node in case it is already defined

    Returns:
        str: name of the node
    """
    if isinstance(node, DelayedObject) and node_name is None:
        try:
            node_name = node._function.__name__
        except TypeError:
            node_name = str(node).replace("<", "").replace(" object at ", "")
    if node_name is None:
        try:
            node_name = node.__name__
        except AttributeError:
            node_name = str(type(node))
    try:
        return node_name + "_" + get_hash(binary=cloudpickle.dumps(node))
    except TypeError:
        return node_name


def recursive_dict_resolve(input_dict: dict) -> dict:
    """
    Recursively resolve the dictionary to call result() on all objects of type DelayedObject

    Args:
        input_dict (dict): dictionary to recursively resolve

    Returns:
        dict: resolved dictionary
    """
    output_dict = {}
    for k, v in input_dict.items():
        if isinstance(v, DelayedObject):
            output_dict[k] = v.pull()
        elif isinstance(v, dict):
            output_dict[k] = recursive_dict_resolve(input_dict=v)
        elif isinstance(v, list):
            output_dict[k] = list(
                recursive_dict_resolve(
                    input_dict={k: v for k, v in enumerate(v)}
                ).values()
            )
        else:
            output_dict[k] = v
    return output_dict


class Selector:
    def __init__(self, obj: object, selector: str):
        self._obj = obj
        self._selector = selector

    def __getattr__(self, name: str):
        if self._selector == "files" and name in self._obj._output_file_lst:
            obj_copy = self._obj.__copy__()
            obj_copy._output_file = name
            return obj_copy
        elif self._selector == "output" and name in self._obj._output_key_lst:
            obj_copy = self._obj.__copy__()
            obj_copy._output_key = name
            return obj_copy
        else:
            return self.__getattribute__(name)


class DelayedObject:
    def __init__(
        self,
        function: callable,
        *args,
        output_key: Optional[str] = None,
        output_file: Optional[str] = None,
        output_file_lst: list = [],
        output_key_lst: list = [],
        list_length: Optional[int] = None,
        list_index: Optional[int] = None,
        input_prefix_key: Optional[str] = None,
        **kwargs,
    ):
        self._input = {}
        self._function = function
        try:
            self._input.update(
                inspect.signature(self._function).bind(*args, **kwargs).arguments
            )
        except TypeError:
            pass
        self.__name__ = "DelayedObject"
        self._result = None
        self._python_function = None
        self._server = Server()
        self._output_key = output_key
        self._output_file = output_file
        self._output_key_lst = output_key_lst
        self._output_file_lst = output_file_lst
        self._list_length = list_length
        self._list_index = list_index
        self._input_prefix_key = input_prefix_key

    @property
    def input(self):
        if self._input_prefix_key is not None and self._input_prefix_key in self._input:
            return self._input[self._input_prefix_key]
        else:
            return self._input

    @property
    def server(self):
        return self._server

    def draw(self):
        node_dict, edge_lst = self.get_graph()
        draw(node_dict=node_dict, edge_lst=edge_lst)

    def get_python_result(self):
        if isinstance(self._result, dict) and self._output_key is not None:
            return self._result[str(self._output_key)]
        elif isinstance(self._result, list):
            if self._list_index is not None:
                return self._result[self._list_index]
            elif self._output_key is not None:
                return self._result[int(self._output_key)]
            else:
                return self._result
        elif self._output_key is not None:
            return getattr(self._result.output, self._output_key)
        else:
            return self._result

    def get_file_result(self):
        return getattr(self._result.files, self._output_file)

    def pull(self):
        if self._result is None:
            self._input.update({"server_obj": self.server})
            self._result = evaluate_function(
                funct=self._function, input_dict=self._input
            )
        if self._output_key is not None:
            return self.get_python_result()
        elif self._output_file is not None:
            return self.get_file_result()
        elif isinstance(self._result, list) and self._list_index is not None:
            return self._result[self._list_index]
        elif isinstance(self._result, dict) and self._list_index is not None:
            return self._result[str(self._list_index)]
        else:
            return self._result

    def get_graph(self):
        return get_graph(obj=self, nodes_dict={}, edges_lst=[], link_node=None)

    def __copy__(self):
        obj_copy = DelayedObject(
            function=self._function,
            output_key=self._output_key,
            output_file=self._output_file,
            output_file_lst=self._output_file_lst,
            output_key_lst=self._output_key_lst,
            list_length=self._list_length,
            list_index=self._list_index,
            input_prefix_key=self._input_prefix_key,
        )
        obj_copy._python_function = self._python_function
        obj_copy._input = self._input
        obj_copy._result = self._result
        obj_copy._server.from_dict(self._server.to_dict())
        return obj_copy

    def __getattr__(self, name):
        if name in ["files", "output"]:
            return Selector(obj=self, selector=name)
        else:
            return self.__getattribute__(name)

    def __iter__(self):
        if self._list_length is not None and self._list_index is None:
            for i in range(self._list_length):
                obj = self.__copy__()
                obj._list_index = i
                yield obj
        else:
            raise TypeError(
                "'DelayedObject' object is not iterable, when self._list_length = None."
            )
