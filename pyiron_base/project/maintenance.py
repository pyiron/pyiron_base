import importlib
import os
import pkgutil
from _warnings import warn

import pandas
from git import Repo, InvalidGitRepositoryError

from pyiron_base import state, get_database_statistics


class Maintenance:
    """
    The purpose of maintenance class is to provide
    some measures of perfomance for pyiron, whether local to the project
    or global (describing the status of pyiron on the running machine)
    """
    def __init__(self, project):
        """
        initialize the local and global attributes
        """
        self._project = project
        self._global = GlobalMaintenance()
        self._update = UpdateMaintenance(self._project)
        self._local = None

    @property
    def global_status(self):
        return self._global

    @property
    def update(self):
        return self._update

    @staticmethod
    def get_repository_status():

        """
        Finds the hashes and versions for every `pyiron` module available.

        Returns:
            pandas.DataFrame: The name of each module and the hash and version for its current git head.
        """
        module_names = [name for _, name, _ in pkgutil.iter_modules() if name.startswith("pyiron")]

        report = pandas.DataFrame(columns=['Module', 'Git head', 'Version'], index=range(len(module_names)))
        for i, name in enumerate(module_names):
            module = importlib.import_module(name)
            try:
                repo = Repo(os.path.dirname(os.path.dirname(module.__file__)))
                hash_ = repo.head.reference.commit.hexsha
            except InvalidGitRepositoryError:
                hash_ = 'Not a repo'
            if hasattr(module, '__version__'):
                version = module.__version__
            else:
                version = "not defined"
            report.loc[i] = [name, hash_, version]

        return report


class UpdateMaintenance:
    def __init__(self, project):
        self._project = project

    def base_v0_3_to_v0_4(self, project=None):
        """ Update hdf files written with pyiron_base-0.3.x to pyiron_base-0.4.x

        pyiron_base<=0.3.9 has a bug that writes all arrays with dtype=object even
        numeric ones.  As a fix pyiron_base=0.4.0 introduces a conversion when reading
        such arrays, but does not automatically save them.  This conversion script
        simply goes over all jobs and rewrites their HDF5 files, since it's read with
        the correct dtype, this then writes this correct dtype.

        Args:
            project(None/pyiron_project/"all"): The project to be converted from 0.3 to 0.4 ; default: current project
                if "all" is provided, pyiron tries to find all projects using the PROJECT_PATHS defined in the
                configuration.
        """
        if project is None:
            projects = [self._project]
        elif isinstance(project, list):
            projects = project
        elif project == 'all':
            projects = [self._project.__class__(path) for path in state.settings.configuration['project_paths']]
        else:
            projects = [project]

        for pr in projects:
            for j in pr.iter_jobs(convert_to_object=False, recursive=True):
                j.project_hdf5.rewrite_hdf5(j.name)


class GlobalMaintenance:
    def __init__(self):
        """
        initialize the flag self._check_postgres, to control whether pyiron is
        set to communicate with a postgres database.
        """
        connection_string = state.database.sql_connection_string
        if "postgresql" not in connection_string:
            warn(
                """
                The database statistics is only available for a Postgresql database
                """
            )
            self._check_postgres = False
        else:
            self._check_postgres = True

    def get_database_statistics(self):
        if self._check_postgres:
            return get_database_statistics()
        else:
            raise RuntimeError(
                """
                The detabase statistics is only available for a Postgresql database
                """
            )
