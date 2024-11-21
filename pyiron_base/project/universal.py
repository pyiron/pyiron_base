from pyiron_base.project.decorator import job
from pyiron_base.project.delayed import DelayedObject


def _remove_server_obj(nodes_dict, edges_lst):
    server_lst = [k for k in nodes_dict.keys() if k.startswith("server_obj_")]
    for s in server_lst:
        del nodes_dict[s]
        edges_lst = [ep for ep in edges_lst if s not in ep]
    return nodes_dict, edges_lst


def _get_nodes(connection_dict, delayed_object_updated_dict):
    return {
        connection_dict[k]: v._python_function
        if isinstance(v, DelayedObject) else v
        for k, v in delayed_object_updated_dict.items()
    }


def _get_unique_objects(nodes_dict, edges_lst):
    delayed_object_dict = {
        k:v for k, v in nodes_dict.items() if isinstance(v, DelayedObject)
    }
    unique_lst = []
    delayed_object_updated_dict, match_dict = {}, {}
    for dobj in delayed_object_dict.keys():
        match = False
        for obj in unique_lst:
            if delayed_object_dict[dobj]._input == delayed_object_dict[obj]._input:
                delayed_object_updated_dict[obj] = delayed_object_dict[obj]
                match_dict[dobj] = obj
                match = True
                break
        if not match:
            unique_lst.append(dobj)
            delayed_object_updated_dict[dobj] = delayed_object_dict[dobj]
    delayed_object_updated_dict.update({
        k:v for k, v in nodes_dict.items() if not isinstance(v, DelayedObject)
    })
    return delayed_object_updated_dict, match_dict


def _get_connection_dict(delayed_object_updated_dict, match_dict):
    new_obj_dict = {}
    connection_dict = {}
    lookup_dict = {}
    for i, [k, v] in enumerate(delayed_object_updated_dict.items()):
        new_obj_dict[i] = v
        connection_dict[k] = i
        lookup_dict[i] = k

    for k, v in match_dict.items():
        if v in connection_dict.keys():
            connection_dict[k] = connection_dict[v]

    return connection_dict, lookup_dict


def _get_edges_dict(edges_lst, nodes_dict, connection_dict, lookup_dict):
    edges_dict_lst = []
    existing_connection_lst = []
    for ep in edges_lst:
        input_name, output_name = ep
        target = connection_dict[input_name]
        target_handle = "_".join(output_name.split("_")[:-1])
        connection_name = lookup_dict[target] + "_" + target_handle
        if connection_name not in existing_connection_lst:
            output = nodes_dict[output_name]
            if isinstance(output, DelayedObject):
                edges_dict_lst.append({
                    "target": target,
                    "targetHandle": target_handle,
                    "source": connection_dict[output_name],
                    "sourceHandle": output._output_key,
                })
            else:
                edges_dict_lst.append({
                    "target": target,
                    "targetHandle": target_handle,
                    "source": connection_dict[output_name],
                    "sourceHandle": None,
                })
            existing_connection_lst.append(connection_name)
    return edges_dict_lst


def _get_kwargs(lst):
    return {t['targetHandle']: {'source': t['source'], 'sourceHandle': t['sourceHandle']} for t in lst}


def _group_edges(edges_lst):
    edges_sorted_lst = sorted(edges_lst, key=lambda x: x['target'], reverse=True)
    total_lst, tmp_lst = [], []
    target_id = edges_sorted_lst[0]['target']
    for ed in edges_sorted_lst:
        if target_id == ed["target"]:
            tmp_lst.append(ed)
        else:
            total_lst.append((target_id, _get_kwargs(lst=tmp_lst)))
            target_id = ed["target"]
            tmp_lst = [ed]
    total_lst.append((target_id, _get_kwargs(lst=tmp_lst)))
    return total_lst


def _get_source_handles(edges_lst):
    source_handle_dict = {}
    for ed in edges_lst:
        if ed['source'] not in source_handle_dict.keys():
            source_handle_dict[ed['source']] = [ed['sourceHandle']]
        else:
            source_handle_dict[ed['source']].append(ed['sourceHandle'])
    return source_handle_dict


def _get_source(nodes_dict, delayed_object_dict, source, sourceHandle):
    if source in delayed_object_dict.keys():
        return delayed_object_dict[source].__getattr__("output").__getattr__(sourceHandle)
    else:
        return nodes_dict[source]


def _get_delayed_object_dict(total_lst, nodes_dict, source_handle_dict, pyiron_project):
    delayed_object_dict = {}
    for item in total_lst:
        key, input_dict = item
        kwargs = {
            k: _get_source(
                nodes_dict=nodes_dict,
                delayed_object_dict=delayed_object_dict,
                source=v["source"],
                sourceHandle=v["sourceHandle"],
            )
            for k, v in input_dict.items()
        }
        delayed_object_dict[key] = job(
            funct=nodes_dict[key],
            output_key_lst=source_handle_dict.get(key, []),
        )(**kwargs, pyiron_project=pyiron_project)
    return delayed_object_dict


def execute_workflow_with_pyiron(nodes_dict, edges_lst, pyiron_project):
    delayed_object_dict = _get_delayed_object_dict(
        total_lst=_group_edges(edges_lst),
        nodes_dict=nodes_dict,
        source_handle_dict=_get_source_handles(edges_lst),
        pyiron_project=pyiron_project,
    )
    return delayed_object_dict[list(delayed_object_dict.keys())[-1]].pull()


def convert_workflow(nodes_dict, edges_lst):
    nodes_dict, edges_lst = _remove_server_obj(
        nodes_dict=nodes_dict,
        edges_lst=edges_lst,
    )
    delayed_object_updated_dict, match_dict = _get_unique_objects(
        nodes_dict=nodes_dict,
        edges_lst=edges_lst,
    )
    connection_dict, lookup_dict = _get_connection_dict(
        delayed_object_updated_dict=delayed_object_updated_dict,
        match_dict=match_dict,
    )
    universal_nodes = _get_nodes(
        connection_dict=connection_dict,
        delayed_object_updated_dict=delayed_object_updated_dict,
    )
    universal_edges = _get_edges_dict(
        edges_lst=edges_lst,
        nodes_dict=nodes_dict,
        connection_dict=connection_dict,
        lookup_dict=lookup_dict,
    )
    return universal_nodes, universal_edges