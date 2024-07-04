import inspect
import hashlib
import re
import cloudpickle


def get_function_parameter_dict(funct):
    return {
        k: None if v.default == inspect._empty else v.default
        for k, v in inspect.signature(funct).parameters.items()
    }


def get_hash(binary):
    # Remove specification of jupyter kernel from hash to be deterministic
    binary_no_ipykernel = re.sub(b"(?<=/ipykernel_)(.*)(?=/)", b"", binary)
    return str(hashlib.md5(binary_no_ipykernel).hexdigest())


def get_graph(obj, obj_name=None, nodes_dict={}, edges_lst=[], link_node=None):
    if isinstance(obj, DelayedObject):
        try:
            obj_name = (
                obj._function.__name__ + "_" + get_hash(binary=cloudpickle.dumps(obj))
            )
        except TypeError:
            obj_name = (
                str(obj).replace("<", "").replace("<", "").replace(" object at ", "")
            )
    if obj_name is None:
        try:
            obj_name = obj.__name__
        except AttributeError:
            obj_name = str(type(obj))
    try:
        node_name = obj_name + "_" + get_hash(binary=cloudpickle.dumps(obj))
    except TypeError:
        node_name = obj_name
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
                obj_name=k,
                nodes_dict=nodes_dict,
                edges_lst=edges_lst,
                link_node=node_name,
            )
    return nodes_dict, edges_lst


def recursive_dict_resolve(input_dict):
    output_dict = {}
    for k, v in input_dict.items():
        if isinstance(v, DelayedObject):
            output_dict[k] = v.result()
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


def draw(node_dict: dict, edge_lst: list):
    from IPython.display import SVG, display
    import matplotlib.pyplot as plt
    import networkx as nx

    graph = nx.DiGraph()
    for k, v in node_dict.items():
        graph.add_node(k, label=str(k).rsplit("_", 1)[0] + "=" + str(v))
    for edge in edge_lst:
        graph.add_edge(edge[1], edge[0])
    svg = nx.nx_agraph.to_agraph(graph).draw(prog="dot", format="svg")
    display(SVG(svg))
    plt.show()


class Selector:
    def __init__(self, obj, selector):
        self._obj = obj
        self._selector = selector

    def __getattr__(self, name):
        if self._selector == "files" and name in self._obj._output_file_lst:
            obj_copy = self._obj.__copy__()
            obj_copy._output_file = name
            return obj_copy
        elif self._selector == "output" and name in self._obj._output_key_lst:
            obj_copy = self._obj.__copy__()
            obj_copy._output_key = name
            return obj_copy
        else:
            raise AttributeError()


class DelayedObject:
    def __init__(
        self,
        function,
        *args,
        output_key=None,
        output_file=None,
        output_file_lst=[],
        output_key_lst=[],
        list_length=None,
        list_index=None,
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
        self._output_key = output_key
        self._output_file = output_file
        self._output_key_lst = output_key_lst
        self._output_file_lst = output_file_lst
        self._list_length = list_length
        self._list_index = list_index

    def get_python_result(self):
        return getattr(self._result.output, self._output_key)

    def get_file_result(self):
        return getattr(self._result.files, self._output_file)

    def result(self):
        if self._result is None:
            input_dict = recursive_dict_resolve(input_dict=self._input)
            if "kwargs" in input_dict.keys():
                input_dict.update(input_dict.pop("kwargs"))
            if "args" in input_dict.keys():
                args = input_dict.pop("args")
                self._result = self._function(*args, **input_dict)
            else:
                self._result = self._function(**input_dict)
        if self._output_key is not None:
            return self.get_python_result()
        elif self._output_file is not None:
            return self.get_file_result()
        elif self._list_index is not None:
            return self._result[self._list_index]
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
        )
        obj_copy._input = self._input
        obj_copy._result = self._result
        return obj_copy

    def __getattr__(self, name):
        if name in ["files", "output"]:
            return Selector(obj=self, selector=name)
        else:
            raise AttributeError()

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
