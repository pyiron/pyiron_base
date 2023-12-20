import numpy as np
import h5io
import h5py
from pyiron_base.utils.error import retry
from typing import Union
import warnings


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
    """
    Read data from HDF5 file

    Args:
        fname (str): Name of the file on disk, or file-like object.  Note: for files created with the 'core' driver,
                     HDF5 still requires this be non-empty.
        title (str): The top-level directory name to use. Typically it is useful to make this your package name,
                     e.g. ``'mnepython'``.
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
        title (str): The top-level directory name to use. Typically it is useful to make this your package name,
                     e.g. ``'mnepython'``.
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
        path (str): The top-level directory name to use. Typically it is useful to make this your package name,
                     e.g. ``'mnepython'``.
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


def write_dict_to_hdf(hdf, data_dict, compression=4, slash="error"):
    """
    Write dictionary to HDF5 file

    Args:
        hdf (pyiron_base.storage.hdfio.FileHDFio): HDF5 file object
        data_dict (dict): dictionary of data objects to be stored in the HDF5 file, the keys provide the path inside
                          the HDF5 file and the values the data to be stored in those nodes. The corresponding HDF5
                          groups are created automatically:
                              {
                                  'hdf5root/group/node_name': {},
                                  'hdf5root/group/subgroup/node_name': [...],
                              }
        compression (int): Compression level to use (0-9) to compress data using gzip.
        slash (str): 'error' | 'replace' Whether to replace forward-slashes ('/') in any key found nested within
                      keys in data. This does not apply to the top level name (title). If 'error', '/' is not allowed
                      in any lower-level keys.
    """
    with open_hdf5(hdf.file_name, mode="a") as store:
        for k, v in data_dict.items():
            write_hdf5_with_json_support(
                file_handle=store,
                value=v,
                path=hdf.get_h5_path(k),
                compression=compression,
                slash=slash,
            )


def _check_json_conversion(value):
    """
    Check if the object can be converted to JSON to optimize the HDF5 performance. This can change the data tupe of the
    object which is going to be stored in the HDF5 file.

    Args:
        value (object): Object to be converted.

    Returns:
        object bool: value object and boolean flag to store the dictionary as JSON
    """
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
