"""
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
numeric ones.  As a fix pyiron_base=0.4.1 introduces a conversion when reading
such arrays, but does not automatically save them.  This conversion script
simply goes over all jobs and rewrites their HDF5 files, since it's read with
the correct dtype, this then writes this correct dtype.
"""

import glob
import os
import stat

import h5py
from tqdm.auto import tqdm


def _h5io_bug_check(_, h5obj):
    """
    Checks if the h5obj is a group which has 'data' and 'index' datasets. Signature matches the requirements of
    h5io.File.visititems(callable).

    Args:
        _: not used name of the visititems method on h5io.File
        h5obj: h5io object
    Returns:
        True if an error has been detected - this breaks the visititems loop
        None if no error is detected - proceeding the visititems loop
    """
    if isinstance(h5obj, h5py.Group):
        if "data" in h5obj.keys() and "index" in h5obj.keys():
            if isinstance(h5obj["data"], h5py.Dataset) and isinstance(
                h5obj["index"], h5py.Dataset
            ):
                return True


def detect_bug(file_name):
    """
    Checks whether HDF5 file has at least one group setup like
    /foo       Group
    /foo/data  Dataset {...}
    /foo/index Dataset {...}
    which is how h5io stores dtype=object arrays.  If a file doesn't have any
    of them there's no need to rewrite them.  If there is it might be a
    corrupted record from our bug or a legitimate dtype=object array.  In that
    case just rewrite anyway.
    """

    h5_file = h5py.File(file_name, "r")
    try:
        bug_found = h5_file.visititems(_h5io_bug_check)
    finally:
        h5_file.close()

    return bug_found


def pyiron_base_03x_to_04x(project):
    """
    pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
    numeric ones.  As a fix pyiron_base=0.4.1 introduces a conversion when reading
    such arrays, but does not automatically save them.  This conversion script
    simply goes over all jobs and rewrites their HDF5 files, since it's read with
    the correct dtype, this then writes this correct dtype.
    """
    total_size = 0
    n_files = 0
    for file in glob.iglob(project.path + "**/*.h5", recursive=True):
        n_files += 1
        total_size += os.stat(file)[stat.ST_SIZE]

    if n_files == 0:
        raise ValueError(f"no HDF5 files found in {project.path}!")

    n_proc = 0
    n_skip = 0
    n_err = 0
    with tqdm(total=total_size, unit="B", unit_scale=1) as t:
        for j in project.iter_jobs(
            convert_to_object=False, recursive=True, progress=False
        ):
            n_proc += 1
            try:
                file_size = os.stat(j.project_hdf5.file_name)[stat.ST_SIZE]
            except FileNotFoundError:
                n_err += 1
                print(
                    f"Job {j.name}/{j.id} is in the database, but points to non-existing HDF5 "
                    f"file {j.project_hdf5.file_name}!"
                )
                t.update(0)
                continue

            if detect_bug(j.project_hdf5.file_name):
                try:
                    j.project_hdf5.rewrite_hdf5(j.name)
                except Exception as e:
                    n_err += 1
                    print(f"WARNING: rewriting job {j.name}/{j.id} failed with {e}")
            else:
                n_skip += 1
            t.update(file_size)
    print(f"Total Jobs: {n_proc}\tErrors: {n_err}\tSkipped (no bug detected): {n_skip}")
