# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The :class:`Settings` object controls all the parameters of the pyiron environment that are specific to your particular
configuration: your username, where on the filesystem to look for resources, and all flags necessary to define how
pyiron objects relate to your database (or lack thereof).
It is universally available for import an instantiation, and the python interpreter only ever sees a single instance of
it, so modifications to the :class:`Settings` in one place are available everywhere else that `Settings` gets/has gotten
instantiated.

It is possible to run pyiron only with default behaviour from the `Settings` class itself, but standard practice is to
overwrite part or all of the default configuration by reading information stored on the system.
This is done in an XOR priority order, where input from only one source is used to overwrite the default values:
The highest priority is available only with the `update` method after the `Settings` object already exists, and is to
take values from a user-provided dictionary.
If no such dictionary is provided, or at initialization time then the highest priority is to read values to read from
system environment variables starting with 'PYIRON'.
If none of these except 'PYIRONCONFIG' are found, next `Settings` will try to read a configuration file stored at this
location.
If 'PYIRONCONFIG' was not specified, `Settings` will instead try to read a file at the default location: `~/.pyiron`.
Finally, if none of these were specified, only the default values from the codebase are used.

The configuration can later be updated by calling the `update` method.
Before going through the update cycle specified above, this routine first checks to see if a dictionary was passed in
and if so uses that to update the default configuration instead.

Additionally, if either of the conda flags `'CONDA_PREFIX'` or `'CONDA_DIR'` are system environment variables, they get
`/share/pyiron` appended to them and these values are *appended* to the resource paths.

Finally, :class:`Settings` converts any file paths from your OS to something pyiron-compatible, and does some other
cleaning and consistency checks.
"""

import ast
import os
from configparser import ConfigParser
from pyiron_base.state.logger import logger
from pyiron_base.state.publications import publications
from pathlib import Path
from pyiron_base.generic.util import deprecate, Singleton
from typing import Union, Dict, List
from distutils.util import strtobool
from copy import deepcopy

__author__ = "Jan Janssen, Liam Huber"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Settings(metaclass=Singleton):
    """
    The settings object reads configuration data from the following sources in decreasing order of priority: system
    environment values (starting with 'PYIRON'), a configuration file identified in the PYIRONCONFIG system environment
    variable, or a default configuration file in ~/.pyiron. One (or none) of these is used to overwrite default values
    specified in the codebase.

    Here are the configuration keys as the appear in the python code/config files/system env variables:

        user / USER / PYIRONUSER (str):
        resource_paths / RESOURCE_PATHS / PYIRONRESOURCEPATHS (list):
        project_paths / PROJECT_PATHS / PYIRONPROJECTPATHS (list):
        connection_timeout / CONNECTION_TIMEOUT / PYIRONCONNECTIONTIMEOUT (int):
        sql_connection_string / CONNECTION / PYIRONSQLCONNECTIONSTRING (str):
        sql_table_name / JOB_TABLE / PYIRONSQLTABLENAME (str):
        sql_view_connection_string / - / - (str): Constructed, not available to be set in config files or sys env.
        sql_view_table_name / VIEWER_TABLE / PYIRONSQLVIEWTABLENAME (str):
        sql_view_user / VIEWERUSER / PYIRONSQLVIEWUSER (str):
        sql_view_user_key / VIEWERPASSWD / PYIRONSQLVIEWUSERKEY (str):
        sql_file / FILE / PYIRONSQLFILE (str):
        sql_host / HOST / PYIRONSQHOST (str):
        sql_type / TYPE / PYIRONSQLTYPE ("SQLite"|"Postgres"|"MySQL"): What type of SQL database to use. (Default is
            "SQLite".)
        sql_user_key / PASSWD / PYIRONSQLUSERKEY ():
        sql_database / NAME / PYIRONSQLDATABASE ():
        project_check_enabled / PROJECT_CHECK_ENABLED / PYIRONPROJECTCHECKENABLED (bool):
        disable_database / DISABLE_DATABASE / PYIRONDISABLE (bool): Whether to turn off the database and use a
            file-system-based hierarchy. (Default is False.)

    Properties:
        configuration (dict): Global variables for configuring the pyiron experience.
        resource_paths (list[str]): A shortcut to the configuration value for locations with pyiron resources.
        login_user (str): A shortcut to the configuration value for the user name.
        default_configuration (dict): Default values for configuration items.
        environment_configuration_map (dict): A map between system environment variable names and the configuration.
        file_configuration_map (dict): A map between config file variable names and the configuration.

    Methods:
        update:  After instantiation, the configuration can be refreshed with this method, which optionally takes a
            dictionary (cf keys above) as the primary (overriding) source but otherwise has the same primacy order as
            the initialization.
        convert_path_to_abs_posix: A path converter, since pyiron internally uses posix style regardless of OS.
    """

    def __init__(self):
        self._configuration = None
        self.update()

    @property
    def configuration(self) -> Dict:
        return self._configuration

    def update(self, user_dict: Union[Dict, None] = None) -> None:
        """
        Starting from a clean set of defaults, overwrite with input from exactly one source with the following priority:
        - User input
        - System environment variables
        - A config file at a locations specified in the PYIRONCONFIG system environment variable
        - A config file at ~/.pyiron
        - Nothing, just use defaults.

        Args:
            user_dict (dict): Configuration items
        """
        self._configuration = dict(self.default_configuration)
        env_dict = self._get_config_from_environment()
        file_dict = self._get_config_from_file()
        if user_dict is not None:
            self._update_from_dict(user_dict)
        elif env_dict is not None:
            self._update_from_dict(env_dict)
        elif file_dict is not None:
            self._update_from_dict(file_dict)

        for k in ["CONDA_PREFIX", "CONDA_DIR"]:
            if k in os.environ.keys():
                res_path = os.path.join(os.environ[k], "share", "pyiron")
                if os.path.exists(res_path):
                    self._configuration["resource_paths"].append(
                        self.convert_path_to_abs_posix(res_path)
                    )
                    break  # If the first one is there, don't look for the second

    @property
    def default_configuration(self) -> Dict:
        return deepcopy(
            {
                "user": "pyiron",
                "resource_paths": [],
                "project_paths": [],
                "connection_timeout": 60,
                "sql_connection_string": None,
                "sql_table_name": "jobs_pyiron",
                "sql_view_connection_string": None,
                "sql_view_table_name": None,
                "sql_view_user": None,
                "sql_view_user_key": None,
                "sql_file": self.convert_path_to_abs_posix("~/pyiron.db"),
                "sql_host": None,
                "sql_type": "SQLite",
                "sql_user_key": None,
                "sql_database": None,
                "project_check_enabled": False,
                "disable_database": False,
            }
        )

    @property
    def environment_configuration_map(self) -> Dict:
        return {
            "PYIRONUSER": "user",
            "PYIRONRESOURCEPATHS": "resource_paths",
            "PYIRONPROJECTPATHS": "project_paths",
            "PYIRONCONNECTIONTIMEOUT": "connection_timeout",
            "PYIRONSQLCONNECTIONSTRING": "sql_connection_string",
            "PYIRONSQLTABLENAME": "sql_table_name",
            "PYIRONSQLVIEWCONNECTIONSTRING": "INVALID_KEY_PYIRONSQLVIEWCONNECTIONSTRING",  # Constructed, not settable
            "PYIRONSQLVIEWTABLENAME": "sql_view_table_name",
            "PYIRONSQLVIEWUSER": "sql_view_user",
            "PYIRONSQLVIEWUSERKEY": "sql_view_user_key",
            "PYIRONSQLFILE": "sql_file",
            "PYIRONSQHOST": "sql_host",
            "PYIRONSQLTYPE": "sql_type",
            "PYIRONSQLUSERKEY": "sql_user_key",
            "PYIRONSQLDATABASE": "sql_database",
            "PYIRONPROJECTCHECKENABLED": "project_check_enabled",
            "PYIRONDISABLE": "disable_database",
        }

    @property
    def file_configuration_map(self) -> Dict:
        return {
            "USER": "user",
            "RESOURCE_PATHS": "resource_paths",
            "PROJECT_PATHS": "project_paths",
            "TOP_LEVEL_DIRS": "project_paths",  # For backwards compatibility
            "CONNECTION_TIMEOUT": "connection_timeout",
            "CONNECTION": "sql_connection_string",
            "JOB_TABLE": "sql_table_name",
            "SQL_VIEW_CONNECTION_STRING": "INVALID_KEY_SQL_VIEW_CONNECTION_STRING",  # Constructed, not settable
            "VIEWER_TABLE": "sql_view_table_name",
            "VIEWERUSER": "sql_view_user",
            "VIEWERPASSWD": "sql_view_user_key",
            "FILE": "sql_file",
            "DATABASE_FILE": "sql_file",  # Alternative name
            "HOST": "sql_host",
            "TYPE": "sql_type",
            "PASSWD": "sql_user_key",
            "NAME": "sql_database",
            "PROJECT_CHECK_ENABLED": "project_check_enabled",
            "DISABLE_DATABASE": "disable_database",
        }

    @staticmethod
    def convert_path_to_abs_posix(path: str) -> str:
        """
        Convert path to an absolute POSIX path

        Args:
            path (str): input path.

        Returns:
            str: absolute path in POSIX format
        """
        return (
            Path(path.strip())
            .expanduser()
            .resolve()
            .absolute()
            .as_posix()
            .replace("\\", "/")
        )

    @property
    def login_user(self) -> str:
        """
        Get the username of the current user

        Returns:
            str: username
        """
        return self._configuration["user"]

    @property
    def resource_paths(self) -> List[str]:
        """
        Paths for pyiron resources, e.g. executables, queue adapter config files, etc.

        Returns:
            list: path of paths
        """
        return self._configuration["resource_paths"]

    @property
    def _valid_sql_types(self) -> List[str]:
        return ["SQLite", "Postgres", "MySQL", "SQLalchemy"]

    def _validate_sql_configuration(self, config: Dict) -> None:
        try:
            sql_type = config["sql_type"]
            if sql_type in ["Postgres", "MySQL"]:
                required_keys = ["user", "sql_user_key", "sql_host", "sql_database"]
                if not all([k in config.keys() for k in required_keys]):
                    raise ValueError(
                        f"For SQL type {sql_type}, {required_keys} are all required but got {config.keys()}"
                    )
            elif sql_type == "SQLite":
                sql_file = config["sql_file"]
                if sql_file is None:
                    # SQLite is raising ugly error messages when the database directory does not exist.
                    raise ValueError(
                        "For sql_type SQLite, the sql_file must not be None"
                    )
                elif os.path.dirname(sql_file) != "" and not os.path.exists(
                    os.path.dirname(sql_file)
                ):
                    os.makedirs(os.path.dirname(sql_file))
            elif (
                sql_type == "SQLalchemy"
                and "sql_connection_string" not in config.keys()
            ):
                raise ValueError(
                    "sql_type was SQLalchemy but did not find a sql_connection_string setting."
                )
            elif sql_type not in self._valid_sql_types:
                raise ValueError(
                    f"sql_type {sql_type} not recognized, please choose among {self._valid_sql_types}"
                )
        except KeyError:
            pass

    @staticmethod
    def _validate_viewer_configuration(config: Dict) -> None:
        key_group = ["sql_view_table_name", "sql_view_user", "sql_view_user_key"]
        present = [k in config.keys() for k in key_group]
        if any(present):
            if not all(present):
                raise ValueError(
                    f"If any of {key_group} is included they all must be, but got {config.keys()}"
                )
            if "sql_type" not in config or config["sql_type"] != "Postgres":
                # Note: This requirement is *implicit* when the sql_view_connection_string is constructed
                #       I don't actually understand the constraint, I am just making it *explicit* as I refactor. -Liam
                raise ValueError("Got sql_view arguments, but sql_type is not Postgres")

    @staticmethod
    def _validate_no_database_configuration(config: Dict) -> None:
        if "disable_database" in config.keys() and config["disable_database"]:
            if (
                "project_check_enabled" in config.keys()
                and config["project_check_enabled"]
            ):
                raise ValueError(
                    "When the database is disabled 'disable_database=True' the project "
                    + "check cannot be enabled, so you have to set 'project_check_enabled=False'."
                )
            if "project_paths" in config.keys() and len(config["project_paths"]) > 0:
                raise ValueError(
                    "When the database is disabled 'disable_database=True' the project "
                    + "paths list should be empty 'project_paths=[]'. Currently it is: "
                    + str(config["project_paths"])
                )

    def _get_config_from_environment(self) -> Union[Dict, None]:
        config = {}
        for k, v in os.environ.items():
            try:
                config[self.environment_configuration_map[k]] = v
            except KeyError:
                pass
        config = self._fix_boolean_var_in_config(config=config)
        return config if len(config) > 0 else None

    def _get_config_from_file(self) -> Union[Dict, None]:
        if "PYIRONCONFIG" in os.environ.keys():
            config_file = os.environ["PYIRONCONFIG"]
        else:
            config_file = os.path.expanduser(os.path.join("~", ".pyiron"))

        if os.path.isfile(config_file):
            parser = ConfigParser(inline_comment_prefixes=(";",), interpolation=None)
            parser.read(config_file)
            config = {}
            for sec_name, section in parser.items():
                for k, v in section.items():
                    try:
                        config[self.file_configuration_map[k.upper()]] = v
                    except KeyError:
                        pass
            config = self._fix_boolean_var_in_config(config=config)
        else:
            config = None
        return config

    def _update_from_dict(self, config: Dict, map_: Union[None, Dict] = None) -> None:
        """
        Overwrite values of the configuration dictionary based on a new dictionary.

        Non-string non-None items are converted to the expected type and paths are converted to absolute POSIX paths.
        """
        self._validate_sql_configuration(config=config)
        self._validate_viewer_configuration(config=config)
        self._validate_no_database_configuration(config=config)

        for key, value in config.items():
            key = key if map_ is None else map_[key]

            if key in ["resource_paths", "project_paths"]:
                self._configuration[key] = self._convert_to_list_of_paths(
                    value, ensure_ends_with="/" if key == "project_paths" else None
                )
            elif key == "connection_timeout":
                self._configuration[key] = int(value)
            elif key == "sql_file":
                self._configuration[key] = self.convert_path_to_abs_posix(value)
            elif key in ["project_check_enabled", "disable_database"]:
                self._configuration[key] = (
                    value if isinstance(value, bool) else strtobool(value)
                )
            elif key not in self._configuration.keys():
                raise KeyError(
                    f"Got unexpected configuration key {key}, please choose from among {self._configuration.keys()}"
                )
            else:
                self._configuration[key] = value

    def _convert_to_list_of_paths(
        self, paths: Union[str, List[str]], ensure_ends_with: Union[None, str] = None
    ) -> List[str]:
        if isinstance(paths, str):
            paths = paths.replace(",", os.pathsep).split(os.pathsep)
        return [
            self.convert_path_to_abs_posix(p)
            if ensure_ends_with is None
            or self.convert_path_to_abs_posix(p).endswith(ensure_ends_with)
            else self.convert_path_to_abs_posix(p) + ensure_ends_with
            for p in paths
        ]

    @property
    # @deprecate("Use pyiron_base.state.state.logger")
    def logger(self):
        return logger

    @property
    # @deprecate("Use pyiron_base.state.state.queue_adapter")
    def queue_adapter(self):
        from pyiron_base.state import state

        return state.queue_adapter

    @property
    # @deprecate("Use pyiron_base.state.state.publications.list()")
    def publication_lst(self):
        """
        List of publications currently in use.

        Returns:
            list: list of publications
        """
        return publications.list()

    # @deprecate("Use pyiron_base.state.state.publications.add")
    def publication_add(self, pub_dict):
        """
        Add a publication to the list of publications

        Args:
            pub_dict (dict): The key should be the name of the code used and the value a list of publications to cite.
        """
        return publications.add(pub_dict)

    @property
    # @deprecate("Use pyiron_base.state.state.publications.pyiron_publication")
    def publication(self):
        return publications.pyiron_publication

    @staticmethod
    def _fix_boolean_var_in_config(config):
        for k, v in config.items():
            if k in ["project_check_enabled", "disable_database"]:
                config[k] = ast.literal_eval(v)
        return config


settings = Settings()
