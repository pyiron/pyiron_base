"""
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
numeric ones.  As a fix pyiron_base=0.4.0 introduces a conversion when reading
such arrays, but does not automatically save them.  This conversion script
simply goes over all jobs and rewrites their HDF5 files, since it's read with
the correct dtype, this then writes this correct dtype.
"""

import sys

from pyiron_base import Project

pr = Project(sys.argv[1])
for j in pr.iter_jobs(convert_to_object=False, recursive=True):
    j.project_hdf5.rewrite_hdf5(j.name)
