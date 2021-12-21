import importlib
import os
import pkgutil
from _warnings import warn

import pandas
from git import Repo, InvalidGitRepositoryError

from pyiron_base import state
from pyiron_base.database.performance import get_database_statistics


class Maintenance:
    """
    The purpose of maintenance class is to provide
    some measures of perfomance for pyiron, whether local to the project
    or global (describing the status of pyiron on the running machine)
    """

    def __init__(self):
        """
        initialize the local and global attributes
        """
        self._global = GlobalMaintenance()
        self._local = None

    @property
    def global_status(self):
        return self._global

    @staticmethod
    def get_repository_status():

        """
        Finds the hashes and versions for every `pyiron` module available.

        Returns:
            pandas.DataFrame: The name of each module and the hash and version for its current git head.
        """
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
                hash_ = repo.head.reference.commit.hexsha
            except InvalidGitRepositoryError:
                hash_ = "Not a repo"
            if hasattr(module, "__version__"):
                version = module.__version__
            else:
                version = "not defined"
            report.loc[i] = [name, hash_, version]

        return report


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