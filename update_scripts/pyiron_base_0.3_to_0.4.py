"""
pyiron_base<=0.3.10 has a bug that writes all arrays with dtype=object even
numeric ones.  As a fix pyiron_base=0.4.1 introduces a conversion when reading
such arrays, but does not automatically save them.  This conversion script
simply goes over all jobs and rewrites their HDF5 files, since it's read with
the correct dtype, this then writes this correct dtype.
"""

import sys
from pyiron_base import Project

from pyiron_base.project.update.pyiron_base_03x_to_04x import pyiron_base_03x_to_04x

if __name__ == "__main__":
    pr = Project(sys.argv[1])
    pyiron_base_03x_to_04x(pr)
