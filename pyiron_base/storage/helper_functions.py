import h5io
from h5io_browser.base import _check_json_conversion, _open_hdf, _read_hdf
import h5py
import posixpath
from pyiron_base.utils.error import retry


def write_hdf5(
    fname,
    data,
    overwrite=False,
    compression=4,
    title="h5io",
    slash="error",
    use_json=False,
):
    """
    Write data to HDF5 file

    Args:
        fname (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core' driver,
                     HDF5 still requires this be non-empty.
        data (object): Object to write. Can be of any of these types: {ndarray, dict, list, tuple, int, float, str,
                       datetime, timezone} Note that dict objects must only have ``str`` keys. It is recommended
                       to use ndarrays where possible, as it is handled most efficiently.
        overwrite (str/bool): True | False | 'update' If True, overwrite file (if it exists). If 'update', appends the
                              title to the file (or replace value if title exists).
        compression (int): Compression level to use (0-9) to compress data using gzip.
        title (str): the HDF5 internal dataset path from which should be read, slashes indicate sub groups
        slash (str): 'error' | 'replace' Whether to replace forward-slashes ('/') in any key found nested within
                      keys in data. This does not apply to the top level name (title). If 'error', '/' is not allowed
                      in any lower-level keys.
        use_json (bool): To accelerate the read and write performance of small dictionaries and lists they can be
                         combined to JSON objects and stored as strings.
    """
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


def write_hdf5_with_json_support(
    value, path, file_handle, compression=4, slash="error"
):
    """
    Write data to HDF5 file and store dictionaries as JSON to optimize performance
    Args:
        value (object): Object to write. Can be of any of these types: {ndarray, dict, list, tuple, int, float, str,
                        datetime, timezone} Note that dict objects must only have ``str`` keys. It is recommended
                        to use ndarrays where possible, as it is handled most efficiently.
        path (str): the HDF5 internal dataset path from which should be read, slashes indicate sub groups
        file_handle (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core'
                           driver, HDF5 still requires this be non-empty.:
        compression (int): Compression level to use (0-9) to compress data using gzip.
        slash (str): 'error' | 'replace' Whether to replace forward-slashes ('/') in any key found nested within
                      keys in data. This does not apply to the top level name (title). If 'error', '/' is not allowed
                      in any lower-level keys.
    """
    value, use_json = _check_json_conversion(value=value)
    try:
        write_hdf5(
            fname=file_handle,
            data=value,
            compression=compression,
            slash=slash,
            use_json=use_json,
            title=path,
            overwrite="update",
        )
    except TypeError:
        raise TypeError(
            "Error saving {} (key {}): DataContainer doesn't support saving elements "
            'of type "{}" to HDF!'.format(value, path, type(value))
        ) from None


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
                hdf_filehandle=store, h5_path=get_h5_path(h5_path=h5_path, name=n), slash=slash
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
            read_dict = resolve_nested_dict(
                group_path=group_path,
                data_dict=get_dict_from_nodes(
                    store=store,
                    h5_path=get_h5_path(h5_path=h5_path, name=group_path),
                    slash=slash,
                ),
            )
            for k, v in read_dict.items():
                if k in output_dict.keys() and isinstance(v, dict):
                    for sk, vs in v.items():
                        output_dict[k][sk] = vs
                else:
                    output_dict[k] = v
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
