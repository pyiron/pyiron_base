from h5io_browser.base import _open_hdf, _read_hdf
import h5py
import posixpath


def list_groups_and_nodes(hdf, h5_path):
    """
    Get the list of groups and list of nodes from an open HDF5 file

    Args:
        hdf (h5py.File): file handle of an open HDF5 file
        h5_path (str): path inside the HDF5 file

    Returns:
        list, list: list of groups and list of nodes
    """
    groups = set()
    nodes = set()
    try:
        h = hdf[h5_path]
        for k in h.keys():
            if isinstance(h[k], h5py.Group):
                groups.add(k)
            else:
                nodes.add(k)
    except KeyError:
        pass
    return list(groups), list(nodes)


def read_dict_from_hdf(
    file_name, h5_path, recursive=False, group_paths=[], slash="ignore"
):
    """
    Read data from HDF5 file into a dictionary - by default only the nodes are converted to dictionaries, additional
    sub groups can be specified using the group_paths parameter.
    Args:
       file_name (str): Name of the file on disk
       h5_path (str): Path to a group in the HDF5 file from where the data is read
       recursive (bool): Load all subgroups recursively
       group_paths (list): list of additional groups to be included in the dictionary, for example:
                           ["input", "output", "output/generic"]
                           These groups are defined relative to the h5_path.
       slash (str): 'ignore' | 'replace' Whether to replace the string {FWDSLASH} with the value /. This does
                    not apply to the top level name (title). If 'ignore', nothing will be replaced.
    Returns:
       dict:     The loaded data. Can be of any type supported by ``write_hdf5``.
    """

    def get_dict_from_nodes(store, h5_path, slash="ignore"):
        """
        Load all nodes from an HDF5 path into a dictionary
        Args:
            store (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core'
                         driver, HDF5 still requires this be non-empty.:
            h5_path (str): Path to a group in the HDF5 file from where the data is read
            slash (str): 'ignore' | 'replace' Whether to replace the string {FWDSLASH} with the value /. This does
                         not apply to the top level name (title). If 'ignore', nothing will be replaced.
        Returns:
            dict:        The loaded data. Can be of any type supported by ``write_hdf5``.
        """
        return {
            n: _read_hdf(
                hdf_filehandle=store,
                h5_path=get_h5_path(h5_path=h5_path, name=n),
                slash=slash,
            )
            for n in list_groups_and_nodes(hdf=store, h5_path=h5_path)[1]
        }

    def resolve_nested_dict(group_path, data_dict):
        """
        Turns a dict with a key containing slashes into a nested dict.  {'/a/b/c': 1} -> {'a': {'b': {'c': 1}
        Args:
            group_path (str): path inside the HDF5 file the data_dictionary was loaded from
            data_dict (dict): dictionary with data loaded from the HDF5 file
        Returns:
            dict: hierarchical dictionary
        """
        groups = group_path.split("/")
        nested_dict = data_dict
        for g in groups[::-1]:
            nested_dict = {g: nested_dict}
        return nested_dict

    def get_groups_hdf(hdf, h5_path):
        """
        Get all sub-groups of a given HDF5 path
        Args:
            hdf (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core'
                       driver, HDF5 still requires this be non-empty.:
            h5_path (str): Path to a group in the HDF5 file from where the data is read
        Returns:
            list: list of HDF5 groups
        """
        try:
            h = hdf[h5_path]
            group_lst = []
            for group in [h[k].name for k in h.keys() if isinstance(h[k], h5py.Group)]:
                group_lst += [group] + get_groups_hdf(hdf=hdf, h5_path=group)
            return group_lst
        except KeyError:
            return []

    def merge_dict(main_dict, add_dict):
        """
        Merge two dictionaries recursively

        Args:
            main_dict (dict): The primary dictionary, the secondary dictionary is merged into
            add_dict (dict): The secondary dictionary which is merged in the primary dictionary

        Returns:
            dict: The merged dictionary with all keys
        """
        for k, v in add_dict.items():
            if k in main_dict.keys() and isinstance(v, dict):
                main_dict[k] = merge_dict(main_dict=main_dict[k], add_dict=v)
            else:
                main_dict[k] = v
        return main_dict

    if recursive and len(group_paths) > 0:
        raise ValueError(
            "Loading subgroups can either be defined by the group paths ",
            group_paths,
            " or by the recursive ",
            recursive,
            " parameter. Specifying both lead to this ValueError.",
        )

    with _open_hdf(file_name, mode="r") as store:
        output_dict = get_dict_from_nodes(store=store, h5_path=h5_path, slash=slash)
        if h5_path == "/" and recursive:
            group_paths = [g[1:] for g in get_groups_hdf(hdf=store, h5_path=h5_path)]
        elif h5_path[0] != "/" and recursive:
            group_paths = [
                g[len("/" + h5_path) + 1 :]
                for g in get_groups_hdf(hdf=store, h5_path="/" + h5_path)
            ]
        elif recursive:
            group_paths = [
                g[len(h5_path) + 1 :]
                for g in get_groups_hdf(hdf=store, h5_path=h5_path)
            ]
        for group_path in group_paths:
            output_dict = merge_dict(
                main_dict=output_dict,
                add_dict=resolve_nested_dict(
                    group_path=group_path,
                    data_dict=get_dict_from_nodes(
                        store=store,
                        h5_path=get_h5_path(h5_path=h5_path, name=group_path),
                        slash=slash,
                    ),
                ),
            )
    return output_dict


def get_h5_path(h5_path, name):
    """
    Combine the current h5_path with the relative path

    Args:
        h5_path (str): absolute path of the node in the hdf5 file
        name (str): relative path to be added to the absolute path

    Returns:
        str: combined path
    """
    return posixpath.join(h5_path, name)
