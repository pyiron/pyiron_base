import importlib
import os
import pkgutil
import warnings

import pandas

from pyiron_base import state
from pyiron_base.database.performance import get_database_statistics
from pyiron_base.project.update.pyiron_base_03x_to_04x import pyiron_base_03x_to_04x


class Maintenance:
    """
    The purpose of maintenance class is to provide
    some measures of perfomance for pyiron, whether local to the project
    or global (describing the status of pyiron on the running machine)
    """

    def __init__(self, project):
        """
        Args:
            (project): pyiron project to do maintenance on
        """
        self._project = project
        self._global = GlobalMaintenance()
        self._update = UpdateMaintenance(self._project)
        self._local = LocalMaintenance(self._project)

    @property
    def global_status(self):
        return self._global

    @property
    def update(self):
        return self._update

    @property
    def local(self):
        return self._local

    @staticmethod
    def get_repository_status():
        """
        Finds the hashes and versions for every `pyiron` module available.

        Returns:
            pandas.DataFrame: The name of each module and the hash and version for its current git head.
        """
        from git import Repo, InvalidGitRepositoryError

        module_names = [
            name for _, name, _ in pkgutil.iter_modules() if name.startswith("pyiron")
        ]

        report = pandas.DataFrame(
            columns=["Module", "Git head", "Version"], index=range(len(module_names))
        )
        for i, name in enumerate(module_names):
            module = importlib.import_module(name)
            try:
                repo = Repo(os.path.dirname(os.path.dirname(module.__file__)))
            except InvalidGitRepositoryError:
                hash_ = "Not a repo"
            else:
                try:
                    hash_ = repo.head.reference.commit.hexsha
                except (ValueError, TypeError):
                    hash_ = "Error while resolving sha"
            if hasattr(module, "__version__"):
                version = module.__version__
            else:
                version = "not defined"
            report.loc[i] = [name, hash_, version]

        return report


class LocalMaintenance:
    def __init__(self, project):
        self._project = project

    def defragment_storage(
        self,
        recursive: bool = True,
        progress: bool = True,
        **kwargs: dict,
    ):
        """
        Iterate over the jobs within the current project and it is sub projects and rewrite the hdf file

        Args:
            recursive (bool): search subprojects [True/False] - True by default
            progress (bool): if True (default), add an interactive progress bar to the iteration
            **kwargs (dict): Optional arguments for filtering with keys matching the project database column name
                            (eg. status="finished"). Asterisk can be used to denote a wildcard, for zero or more
                            instances of any character
        """
        for job in self._project.iter_jobs(
            recursive=recursive, progress=progress, convert_to_object=False, **kwargs
        ):
            hdf = job.project_hdf5
            hdf.rewrite_hdf5(job.name)


class UpdateMaintenance:
    def __init__(self, project):
        self._project = project

    def base_to_current(self, start_version: str, project=None):
        """Runs all updates for pyiron_base to reach the current version.

        Args:
            start_version(str): Version of pyiron_base in the mayor.minor[.patch] format from which to start applying
                the updates.
            project(None/project/list/str): The project(s) to be converted from 0.3 to 0.4 ; default: current project
                One may provide a pyiron Project, a list of pyiron Projects, or a string containing "all" or a valid
                path.
                If "all" is provided, pyiron tries to find all projects using the PROJECT_PATHS defined in the
                configuration.
        """
        mayor, minor = start_version.split(".")[0:2]
        if int(mayor) != 0:
            raise ValueError("Updates to version >0.x.y is not possible.")
        if int(minor) < 4:
            self.base_v0_3_to_v0_4(project)

    def base_v0_3_to_v0_4(self, project=None):
        """Update hdf files written with pyiron_base-0.3.x to pyiron_base-0.4.x

        pyiron_base<=0.3.9 has a bug that writes all arrays with dtype=object even
        numeric ones.  As a fix pyiron_base=0.4.0 introduces a conversion when reading
        such arrays, but does not automatically save them.  This conversion script
        simply goes over all jobs and rewrites their HDF5 files, since it's read with
        the correct dtype, this then writes this correct dtype.

        Args:
            project(None/project/list/str): The project(s) to be converted from 0.3 to 0.4 ; default: current project
                One may provide a pyiron Project, a list of pyiron Projects, or a string containing "all" or a valid
                path.
                If "all" is provided, pyiron tries to find all projects using the PROJECT_PATHS defined in the
                configuration.
        """
        if project is None:
            projects = [self._project]
        elif isinstance(project, list):
            projects = project
        elif project == "all":
            projects = [
                self._project.__class__(path)
                for path in state.settings.configuration["project_paths"]
            ]
        elif isinstance(project, str):
            if os.path.isdir(project):
                projects = [self._project.__class__(project)]
            else:
                raise ValueError(
                    f"{project} is a str but neither 'all' nor a directory."
                )
        else:
            projects = [project]

        if len(projects) == 0:
            warnings.warn(
                f"Provided project {project} lead to 0 projects to be converted."
            )

        for pr in projects:
            try:
                pyiron_base_03x_to_04x(pr)
            except ValueError as e:
                print(f"WARNING: Updating project {project} failed with {e}!")


class GlobalMaintenance:
    def __init__(self):
        """
        initialize the flag self._check_postgres, to control whether pyiron is
        set to communicate with a postgres database.
        """
        connection_string = state.database.sql_connection_string
        if "postgresql" not in connection_string:
            warnings.warn(
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
