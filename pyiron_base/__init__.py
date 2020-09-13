from pyiron_base.project.generic import Project
from pyiron_base.generic.jedi import fix_ipython_autocomplete
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

# jedi fix
fix_ipython_autocomplete()
