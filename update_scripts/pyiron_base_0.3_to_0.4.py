"""
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
numeric ones.  As a fix pyiron_base=0.4.0 introduces a conversion when reading
such arrays, but does not automatically save them.  This conversion script
simply goes over all jobs and rewrites their HDF5 files, since it's read with
the correct dtype, this then writes this correct dtype.
"""

import os
import re
import stat
import sys
import subprocess

from pyiron_base import Project

from tqdm import tqdm

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
    out = subprocess.getoutput(f"h5ls -r {file_name}")
    lines = out.split('\n')
    for i, l in enumerate(lines[:-2]):
        if not l.endswith("Group"):
            continue
        group_name = l.split()[0]
        data_match = re.match(f"^{group_name}/data[ \t]*Dataset {'{.*}'}$", lines[i+1])
        index_match = re.match(f"^{group_name}/index[ \t]*Dataset {'{.*}'}$", lines[i+2])
        if data_match and index_match:
            return True
    return False


if __name__ == "__main__":
    total_size = 0
    for l in subprocess.getoutput(f"find {sys.argv[1]} -regex \".*\.h5\" -exec wc -c '{{}}' \;").split("\n"):
        total_size += int(l.split()[0])

    pr = Project(sys.argv[1])
    n_skip = 0
    n_err  = 0
    with tqdm(total=total_size, unit="B", unit_scale=1) as t:
        for j in pr.iter_jobs(convert_to_object=False, recursive=True, progress=False):
            try:
                file_size = os.stat(j.project_hdf5.file_name)[stat.ST_SIZE]
            except FileNotFoundError:
                n_err += 1
                print(f"Job {j.name}/{j.id} is in the database, but points to non-existing HDF5 file {j.project_hdf5.file_name}!")
                t.update(file_size)
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
    print(f"Errors: {n_err}\tSkipped: {n_skip}")
