import h5io
import h5py
import posixpath
from pyiron_base.utils.error import retry


def read_hdf5(fname, title, slash="ignore"):
    """
    Read data from HDF5 file

    Args:
        fname (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core' driver,
                     HDF5 still requires this be non-empty.
        title (str): the HDF5 internal dataset path from which should be read, slashes indicate sub groups
        slash (str): 'ignore' | 'replace' Whether to replace the string {FWDSLASH} with the value /. This does
                     not apply to the top level name (title). If 'ignore', nothing will be replaced.

    Returns:
        object:     The loaded data. Can be of any type supported by ``write_hdf5``.
    """
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
