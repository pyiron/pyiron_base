import h5io
import h5py
from pyiron_base.utils.error import retry


def open_hdf5(filename, mode="r", swmr=False):
    """
    Open HDF5 file
    Args:
        filename (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core' driver,
                        HDF5 still requires this be non-empty.
        mode (str): r        Readonly, file must exist (default)
                    r+       Read/write, file must exist
                    w        Create file, truncate if exists
                    w- or x  Create file, fail if exists
                    a        Read/write if exists, create otherwise
        swmr (bool): Open the file in SWMR read mode. Only used when mode = 'r'.
    Returns:
        h5py.File: open HDF5 file object
    """
    if swmr and mode != "r":
        store = h5py.File(name=filename, mode=mode, libver="latest")
        store.swmr = True
        return store
    else:
        return h5py.File(name=filename, mode=mode, libver="latest", swmr=swmr)


def read_hdf5(fname, title="h5io", slash="ignore"):
    return retry(
        lambda: h5io.read_hdf5(
            fname=fname,
            title=title,
            slash=slash,
        ),
        error=BlockingIOError,
        msg=f"Two or more processes tried to access the file {fname}.",
        at_most=10,
        delay=1,
    )


def write_hdf5(
    fname,
    data,
    overwrite=False,
    compression=4,
    title="h5io",
    slash="error",
    use_json=False,
):
    retry(
        lambda: h5io.write_hdf5(
            fname=fname,
            data=data,
            overwrite=overwrite,
            compression=compression,
            title=title,
            slash=slash,
            use_json=use_json,
        ),
        error=BlockingIOError,
        msg=f"Two or more processes tried to access the file {fname}.",
        at_most=10,
        delay=1,
    )


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
    print("list_groups_and_nodes():", h5_path, groups, nodes)
    return list(groups), list(nodes)


def read_dict_from_hdf5(hdf, group_paths=[], slash="ignore"):
    """
    Read data from HDF5 file into a dictionary - by default only the nodes are converted to dictionaries, additional
    groups can be specified using the group_paths parameter.

    Args:
       hdf (pyiron_base.storage.hdfio.FileHDFio): HDF5 file object
       group_paths (list): list of additional groups to be included in the dictionary, for example:
                           ["input", "output", "output/generic"]
       slash (str): 'ignore' | 'replace' Whether to replace the string {FWDSLASH} with the value /. This does
                    not apply to the top level name (title). If 'ignore', nothing will be replaced.
    Returns:
       dict:     The loaded data. Can be of any type supported by ``write_hdf5``.
    """

    def get_dict_from_nodes(store, hdf, slash="ignore"):
        return {
            n: read_hdf5(fname=store, title=hdf._get_h5_path(n), slash=slash)
            for n in list_groups_and_nodes(hdf=store, h5_path=hdf.h5_path)[1]
        }

    def resolve_nested_dict(group_path, data_dict):
        group_lst = group_path.split("/")
        if len(group_lst) > 1:
            return {group_lst[0]: resolve_nested_dict(
                group_path='/'.join(group_lst[1:]),
                data_dict=data_dict
            )}
        else:
            return {group_lst[0]: data_dict}

    with open_hdf5(hdf.file_name, mode="r") as store:
        output_dict = get_dict_from_nodes(store=store, hdf=hdf, slash=slash)
        for group_path in group_paths:
            with hdf.open(group_path) as hdf_group:
                output_dict.update(resolve_nested_dict(
                    group_path=group_path,
                    data_dict=get_dict_from_nodes(
                        store=store, hdf=hdf_group, slash=slash
                    )
                ))
    return output_dict
