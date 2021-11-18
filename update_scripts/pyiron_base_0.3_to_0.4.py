"""
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
numeric ones.  As a fix pyiron_base=0.4.0 introduces a conversion when reading
such arrays, but does not automatically save them.  This conversion script
simply goes over all jobs and rewrites their HDF5 files, since it's read with
the correct dtype, this then writes this correct dtype.
"""

import os
import stat
import sys
import subprocess

from pyiron_base import Project

from tqdm import tqdm

if __name__ == "__main__":
    total_size = 0
    for l in subprocess.getoutput(f"find {sys.argv[1]} -regex \".*\.h5\" -exec wc -c '{{}}' \;").split("\n"):
        total_size += int(l.split()[0])

    pr = Project(sys.argv[1])
    with tqdm(total=total_size, unit="B", unit_scale=1) as t:
        for j in pr.iter_jobs(convert_to_object=False, recursive=True, progress=False):
            try:
                j.project_hdf5.rewrite_hdf5(j.name)
            except e:
                print(f"WARNING: rewriting job {j.name} failed with {e}")

            file_size = os.stat(j.project_hdf5.file_name)[stat.ST_SIZE]
            t.update(file_size)
