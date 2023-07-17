from pyiron_base.jobs.datamining import *
import warnings

warnings.warning(
    "pyiron_base.table.datamining is deprecated, import from pyiron_base.job.datamining",
    category=DeprecationWarning,
    stacklevel=1,
)
