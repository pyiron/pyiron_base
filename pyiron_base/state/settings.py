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
import warnings
from configparser import ConfigParser
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Union

from pyiron_snippets.logger import logger
from pyiron_snippets.singleton import Singleton

from pyiron_base.state.publications import publications
from pyiron_base.utils.strtobool import strtobool

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


PYIRON_DICT_NAME = "PYIRON"


class Settings(metaclass=Singleton):
    """The unique settings object (singleton) for the currently running pyiron instance.

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
        sql_file / FILE / PYIRONSQLFILE (str):
        sql_host / HOST / PYIRONSQHOST (str):
        sql_type / TYPE / PYIRONSQLTYPE ("SQLite"|"Postgres"|"MySQL"): What type of SQL database to use. (Default is
            "SQLite".)
        sql_user_key / PASSWD / PYIRONSQLUSERKEY ():
        sql_database / NAME / PYIRONSQLDATABASE ():
        project_check_enabled / PROJECT_CHECK_ENABLED / PYIRONPROJECTCHECKENABLED (bool):
        disable_database / DISABLE_DATABASE / PYIRONDISABLE (bool): Whether to turn off the database and use a
            file-system-based hierarchy. (Default is False.)
        credentials_file / CREDENTIALS_FILE / CREDENTIALSFILE (str): Path to an additional credentials file holding
            credential information. If specified, the values in the credentials_file overwrite the values of other
            sources.
        write_work_dir_warnings / WRITE_WORK_DIR_WARNINGS / PYIRONWRITEWORKDIRWARNINGS (bool): Whether to write
            the working directory warning files to inform users about possibly modified content. (Default is True).
        config_file_permissions_warning / CONFIG_FILE_PERMISSIONS_WARNING / PYIRONCONFIGFILEPERMISSIONSWARNING (bool):
            Whether to print a warning message, when the permission of the .pyiron config file, let others access it.


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
        self._credentials = None
        self.update()

    @property
    def configuration(self) -> Dict:
        return self._configuration

    @property
    def credentials(self) -> Dict:
        return self._credentials

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

        self._credentials = self._add_credentials_from_file()
        self._update_credentials_from_std_pyiron_config()

        if (
            self._configuration["config_file_permissions_warning"]
            and self._configuration["credentials_file"] is not None
            and os.path.exists(self._configuration["credentials_file"])
            and oct(os.stat(self._configuration["credentials_file"]).st_mode)[-2:]
            != "00"
        ):
            logger.warning(
                "Credentials file can be read by other users - check permissions."
            )

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
                "sql_file": self.convert_path_to_abs_posix("~/pyiron.db"),
                "sql_host": None,
                "sql_type": "SQLite",
                "sql_user_key": None,
                "sql_database": None,
                "project_check_enabled": False,
                "disable_database": False,
                "credentials_file": None,
                "write_work_dir_warnings": True,
                "config_file_permissions_warning": True,
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
            "PYIRONSQLFILE": "sql_file",
            "PYIRONSQHOST": "sql_host",
            "PYIRONSQLTYPE": "sql_type",
            "PYIRONSQLUSERKEY": "sql_user_key",
            "PYIRONSQLDATABASE": "sql_database",
            "PYIRONPROJECTCHECKENABLED": "project_check_enabled",
            "PYIRONDISABLE": "disable_database",
            "PYIRONCREDENTIALSFILE": "credentials_file",
            "PYIRONWRITEWORKDIRWARNINGS": "write_work_dir_warnings",
            "PYIRONCONFIGFILEPERMISSIONSWARNING": "config_file_permissions_warning",
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
            "FILE": "sql_file",
            "DATABASE_FILE": "sql_file",  # Alternative name
            "HOST": "sql_host",
            "TYPE": "sql_type",
            "PASSWD": "sql_user_key",
            "NAME": "sql_database",
            "PROJECT_CHECK_ENABLED": "project_check_enabled",
            "DISABLE_DATABASE": "disable_database",
            "CREDENTIALS_FILE": "credentials_file",
            "WRITE_WORK_DIR_WARNINGS": "write_work_dir_warnings",
            "CONFIG_FILE_PERMISSIONS_WARNING": "config_file_permissions_warning",
        }

    @property
    def file_credential_map(self) -> Dict:
        return {
            "PASSWD": "sql_user_key",
        }

    @property
    def environment_credential_map(self) -> Dict:
        return {
            "PYIRONSQLUSERKEY": "sql_user_key",
        }

    @property
    def _credential_keys(self) -> List:
        return list(self.environment_credential_map.values())

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
                elif os.path.dirname(sql_file) != "":
                    os.makedirs(os.path.dirname(sql_file), exist_ok=True)
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
            if k in self.environment_configuration_map:
                config[self.environment_configuration_map[k]] = v
            elif k in self.environment_credential_map:
                config[self.environment_credential_map[k]] = v
        config = self._fix_boolean_var_in_config(config=config)
        return config if len(config) > 0 else None

    def _get_remapped_credential_key(self, k):
        """
        Converts a key to the known key from the file_credential map or returns a .lower() variant of the unknown key.
        This allows to stay consistent with the behavior of our current credentials and adds the possibility to add
        additional credentials to the credentials file without the need to change pyiron_base.
        """
        if k.upper() in self.file_credential_map:
            return self.file_credential_map[k.upper()]
        elif k.upper() in self.file_configuration_map:
            warnings.warn(
                f"pyiron configuration key {k.upper()} used in the credentials file. "
                "This does not take effect in the pyiron configuration!"
            )

        return k.lower()

    def _add_credentials_from_file(self) -> Dict:
        if (
            "credentials_file" in self._configuration
            and self._configuration["credentials_file"] is not None
        ):
            credential_file = self._configuration["credentials_file"]

            # This gets all the entries in the credential file with the headers
            # The key-value pairs in the [DEFAULT] section are added everywhere!
            parser = ConfigParser(inline_comment_prefixes=(";",), interpolation=None)
            parser.read(credential_file)
            credentials = {}
            for sec_name, section in parser.items():
                credentials_w = {}

                for k, v in section.items():
                    credentials_w[self._get_remapped_credential_key(k)] = v
                if len(credentials_w) > 0:
                    credentials[sec_name.upper()] = credentials_w
            return credentials

    def _update_credentials_from_std_pyiron_config(self):
        update_dict = {}
        for key in self.file_credential_map.values():
            if key in self._configuration:
                update_dict[key] = self._configuration[key]

        if len(update_dict) > 0:
            if self._credentials is None:
                self._credentials = {PYIRON_DICT_NAME: update_dict}
            elif PYIRON_DICT_NAME in self._credentials:
                self._credentials[PYIRON_DICT_NAME].update(update_dict)
            else:
                self._credentials[PYIRON_DICT_NAME] = update_dict

    def _get_credentials_from_file(self, config: dict) -> Dict:
        if "credentials_file" in config and config["credentials_file"] is not None:
            credential_file = config["credentials_file"]

            if not os.path.isfile(credential_file):
                raise FileNotFoundError(credential_file)
            credentials = (
                self._parse_config_file(credential_file, self.file_credential_map) or {}
            )
            config.update(credentials)

        return config

    def _get_config_from_file(self) -> Union[Dict, None]:
        if "PYIRONCONFIG" in os.environ.keys():
            config_file = os.environ["PYIRONCONFIG"]
        else:
            config_file = os.path.expanduser(os.path.join("~", ".pyiron"))

        config = self._parse_config_file(config_file, self.file_configuration_map)

        if config is not None:
            config = self._fix_boolean_var_in_config(config=config)

        return config

    @staticmethod
    def _parse_config_file(config_file, map_dict):
        if os.path.isfile(config_file):
            parser = ConfigParser(inline_comment_prefixes=(";",), interpolation=None)
            parser.read(config_file)
            config = {}
            for sec_name, section in parser.items():
                for k, v in section.items():
                    if k.upper() in map_dict:
                        config[map_dict[k.upper()]] = v
            return config
        else:
            return None

    def _update_from_dict(self, config: Dict, map_: Union[None, Dict] = None) -> None:
        """
        Overwrite values of the configuration dictionary based on a new dictionary.

        Non-string non-None items are converted to the expected type and paths are converted to absolute POSIX paths.
        """
        config = self._get_credentials_from_file(config)
        self._validate_sql_configuration(config=config)
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
            elif key not in self._configuration and key not in self._credential_keys:
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
            (
                self.convert_path_to_abs_posix(p)
                if ensure_ends_with is None
                or self.convert_path_to_abs_posix(p).endswith(ensure_ends_with)
                else self.convert_path_to_abs_posix(p) + ensure_ends_with
            )
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
