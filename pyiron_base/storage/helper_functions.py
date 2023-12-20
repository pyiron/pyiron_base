import numpy as np
import h5io
import h5py
from pyiron_base.utils.error import retry
from typing import Union
import warnings


def open_hdf5(filename, mode="r", swmr=False):
    if swmr and mode != "r":
        store = h5py.File(filename, mode=mode, libver="latest")
        store.swmr = True
        return store
    else:
        return h5py.File(filename, mode=mode, libver="latest", swmr=swmr)


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


def write_hdf5_with_json_support(value, path, file_handle):
    value, use_json = _check_json_conversion(value=value)
    try:
        write_hdf5(
            file_handle,
            value,
            title=path,
            overwrite="update",
            use_json=use_json,
        )
    except TypeError:
        raise TypeError(
            "Error saving {} (key {}): DataContainer doesn't support saving elements "
            'of type "{}" to HDF!'.format(value, path, type(value))
        ) from None


def write_dict_to_hdf(hdf, data_dict, groups=[]):
    with open_hdf5(hdf.file_name, mode="a") as store:
        for k, v in data_dict.items():
            if k not in groups:
                write_hdf5_with_json_support(
                    file_handle=store, value=v, path=hdf.get_h5_path(k)
                )
        for group in groups:
            hdf_group = hdf.create_group(group)
            for k, v in data_dict[group].items():
                write_hdf5_with_json_support(
                    file_handle=store, value=v, path=hdf_group.get_h5_path(k)
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
            n: read_hdf5(fname=store, title=hdf.get_h5_path(n), slash=slash)
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


def _check_json_conversion(value):
    use_json = True
    if (
        isinstance(value, (list, np.ndarray))
        and len(value) > 0
        and isinstance(value[0], (list, np.ndarray))
        and len(value[0]) > 0
        and not isinstance(value[0][0], str)
        and _is_ragged_in_1st_dim_only(value)
    ):
        # if the sub-arrays in value all share shape[1:], h5io comes up with a more efficient storage format than
        # just writing a dataset for each element, by concatenating along the first axis and storing the indices
        # where to break the concatenated array again
        value = np.array([np.asarray(v) for v in value], dtype=object)
        use_json = False
    elif isinstance(value, tuple):
        value = list(value)
    return value, use_json


def _is_ragged_in_1st_dim_only(value: Union[np.ndarray, list]) -> bool:
    """
    Checks whether array or list of lists is ragged in the first dimension.

    That means all other dimensions (except the first one) still have to match.

    Args:
        value (ndarray/list): array to check

    Returns:
        bool: True if elements of value are not all of the same shape
    """
    if isinstance(value, np.ndarray) and value.dtype != np.dtype("O"):
        return False
    else:

        def extract_dims(v):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                s = np.shape(v)
            return s[0], s[1:]

        dim1, dim_other = zip(*map(extract_dims, value))
        return len(set(dim1)) > 1 and len(set(dim_other)) == 1
