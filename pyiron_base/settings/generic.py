# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The settings file provides the attributes of the configuration as properties.

The settings object is additionally responsible for the queue adapter, the logger, and the publications list.
"""

import os
import importlib
from configparser import ConfigParser
from pyiron_base.settings.logger import setup_logger

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Singleton(type):
    """
    Implemented with suggestions from

    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if (
            kwargs is not None
            and "config" in kwargs.keys()
            and kwargs["config"] is not None
        ):
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Settings(metaclass=Singleton):
    """
    The settings object can either search for an configuration file and use the default configuration only when no
    other configuration file is found, or it can be forced to use the default configuration file.

    Args:
        config (dict): Provide a dict with the configuration.
    """

    def __init__(self, config=None):
        # Default config dictionary
        self._configuration = {
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
            "sql_file": None,
            "sql_host": None,
            "sql_type": "SQLite",
            "sql_user_key": None,
            "sql_database": None,
            "project_check_enabled": True,
            "disable_database": False,
        }
        self._update_configuration(config)

        # Build the SQLalchemy connection strings from config data
        if not self._configuration["disable_database"]:
            self._configuration = self.convert_database_config(
                config=self._configuration
            )

        self._queue_adapter = None
        self._queue_adapter = self._init_queue_adapter(
            resource_path_lst=self._configuration["resource_paths"]
        )
        self.logger = setup_logger()
        self._publication_lst = {}
        self.publication_add(self.publication)

    @property
    def configuration(self):
        return self._configuration

    def _update_configuration(self, config):
        environment = os.environ
        if "PYIRONCONFIG" in environment.keys():
            config_file = environment["PYIRONCONFIG"]
        else:
            config_file = os.path.expanduser(os.path.join("~", ".pyiron"))

        if os.path.isfile(config_file):
            self._config_parse_file(config_file)
        elif any(["PYIRON" in e for e in environment.keys()]):
            self._configuration = self._get_config_from_environment(
                environment=environment,
                config=self._configuration
            )
        else:
            self._configuration["sql_file"] = "~/pyiron.db"
            self._configuration["project_check_enabled"] = False

        # Take dictionary as primary source - overwrite everything
        self._read_external_config(config=config)
        if "CONDA_PREFIX" in environment.keys() \
                and os.path.exists(os.path.join(environment["CONDA_PREFIX"], "share", "pyiron")):
            self._configuration["resource_paths"].append(os.path.join(environment["CONDA_PREFIX"], "share", "pyiron"))
        elif "CONDA_DIR" in environment.keys() \
                and os.path.exists(os.path.join(environment["CONDA_DIR"], "share", "pyiron")):
            self._configuration["resource_paths"].append(os.path.join(environment["CONDA_DIR"], "share", "pyiron"))

        self._configuration["project_paths"] = [
            convert_path(path) if path.endswith("/") else convert_path(path) + "/"
            for path in self._configuration["project_paths"]
        ]
        self._configuration["resource_paths"] = [
            convert_path(path) for path in self._configuration["resource_paths"]
        ]

    @property
    def queue_adapter(self):
        return self._queue_adapter

    @property
    def publication_lst(self):
        """
        List of publications currently in use.

        Returns:
            list: list of publications
        """
        all_publication = []
        for v in self._publication_lst.values():
            if isinstance(v, list):
                all_publication += v
            else:
                all_publication.append(v)
        return all_publication

    def publication_add(self, pub_dict):
        """
        Add a publication to the list of publications

        Args:
            pub_dict (dict): The key should be the name of the code used and the value a list of publications to cite.
        """
        for key, value in pub_dict.items():
            if key not in self._publication_lst.keys():
                self._publication_lst[key] = value

    @property
    def login_user(self):
        """
        Get the username of the current user

        Returns:
            str: username
        """
        return self._configuration["user"]

    @property
    def resource_paths(self):
        """
        Get the path where the potentials for the individual Hamiltons are located

        Returns:
            list: path of paths
        """
        return self._configuration["resource_paths"]

    # private functions
    @staticmethod
    def _init_queue_adapter(resource_path_lst):
        """
        Initialize the queue adapter if a folder queues is found in one of the resource paths which contains a
        queue configuration file (queue.yaml).

        Args:
            resource_path_lst (list): List of resource paths

        Returns:
            pysqa.QueueAdapter:
        """
        for resource_path in resource_path_lst:
            if (
                os.path.exists(resource_path)
                and "queues" in os.listdir(resource_path)
                and (
                    "queue.yaml" in os.listdir(os.path.join(resource_path, "queues")) or
                    "clusters.yaml" in os.listdir(os.path.join(resource_path, "queues"))
                )
            ):
                queueadapter = getattr(importlib.import_module("pysqa"), "QueueAdapter")
                return queueadapter(directory=os.path.join(resource_path, "queues"))
        return None

    def _config_parse_file(self, config_file):
        """
        Read section in configuration file and return a dictionary with the corresponding parameters.

        Args:
            config_file(str): confi file to parse

        Returns:
            dict: dictionary with the environment configuration
        """
        # load config parser - depending on Python version
        parser = ConfigParser(inline_comment_prefixes=(";",))

        # read config
        parser.read(config_file)

        # load first section or default section [DEFAULT]
        if len(parser.sections()) > 0:
            section = parser.sections()[0]
        else:
            section = "DEFAULT"

        # identify SQL type
        if parser.has_option(section, "TYPE"):
            self._configuration["sql_type"] = parser.get(section, "TYPE")

        # read variables
        if parser.has_option(section, "PROJECT_PATHS"):
            self._configuration["project_paths"] = [
                convert_path(c.strip())
                for c in parser.get(section, "PROJECT_PATHS").split(",")
            ]
        elif parser.has_option(
            section, "TOP_LEVEL_DIRS"
        ):  # for backwards compatibility
            self._configuration["project_paths"] = [
                convert_path(c.strip())
                for c in parser.get(section, "TOP_LEVEL_DIRS").split(",")
            ]
        else:
            ValueError("No project path identified!")

        if parser.has_option(section, "PROJECT_CHECK_ENABLED"):
            self._configuration["project_check_enabled"] = parser.getboolean(section, "PROJECT_CHECK_ENABLED")

        if parser.has_option(section, "DISABLE_DATABASE"):
            self._configuration["disable_database"] = parser.getboolean(section, "DISABLE_DATABASE")

        if parser.has_option(section, "RESOURCE_PATHS"):
            self._configuration["resource_paths"] = [
                convert_path(c.strip())
                for c in parser.get(section, "RESOURCE_PATHS").replace(",", ":").replace(";", ":").split(":")
            ]

        if self._configuration["sql_type"] in ["Postgres", "MySQL"]:
            if (
                parser.has_option(section, "USER")
                & parser.has_option(section, "PASSWD")
                & parser.has_option(section, "HOST")
                & parser.has_option(section, "NAME")
            ):
                self._configuration["user"] = parser.get(section, "USER")
                self._configuration["sql_user_key"] = parser.get(section, "PASSWD")
                self._configuration["sql_host"] = parser.get(section, "HOST")
                self._configuration["sql_database"] = parser.get(section, "NAME")
                self._configuration["sql_file"] = None
            else:
                raise ValueError(
                    "If type Postgres or MySQL are selected the options USER, PASSWD, HOST and NAME are"
                    "required in the configuration file."
                )

            if (
                parser.has_option(section, "VIEWERUSER")
                & parser.has_option(section, "VIEWERPASSWD")
                & parser.has_option(section, "VIEWER_TABLE")
            ):
                self._configuration["sql_view_table_name"] = parser.get(
                    section, "VIEWER_TABLE"
                )
                self._configuration["sql_view_user"] = parser.get(section, "VIEWERUSER")
                self._configuration["sql_view_user_key"] = parser.get(
                    section, "VIEWERPASSWD"
                )
            self._configuration["connection_timeout"] = parser.get(section, "CONNECTION_TIMEOUT", fallback=60)
        elif self._configuration["sql_type"] == "SQLalchemy":
            self._configuration["sql_connection_string"] = parser.get(
                section, "CONNECTION"
            )
            self._configuration["connection_timeout"] = parser.get(section, "CONNECTION_TIMEOUT", fallback=60)
        else:  # finally we assume an SQLite connection
            if parser.has_option(section, "FILE"):
                self._configuration["sql_file"] = parser.get(section, "FILE").replace(
                    "\\", "/"
                )
            if parser.has_option(section, "DATABASE_FILE"):
                self._configuration["sql_file"] = parser.get(
                    section, "DATABASE_FILE"
                ).replace("\\", "/")

        if parser.has_option(section, "JOB_TABLE"):
            self._configuration["sql_table_name"] = parser.get(section, "JOB_TABLE")

    @staticmethod
    def convert_database_config(config):
        # Build the SQLalchemy connection strings
        if config["sql_type"] == "Postgres":
            config["sql_connection_string"] = (
                "postgresql://"
                + config["user"]
                + ":"
                + config["sql_user_key"]
                + "@"
                + config["sql_host"]
                + "/"
                + config["sql_database"]
            )
            if config["sql_view_user"] is not None:
                config["sql_view_connection_string"] = (
                    "postgresql://"
                    + config["sql_view_user"]
                    + ":"
                    + config["sql_view_user_key"]
                    + "@"
                    + config["sql_host"]
                    + "/"
                    + config["sql_database"]
                )
        elif config["sql_type"] == "MySQL":
            config["sql_connection_string"] = (
                "mysql+pymysql://"
                + config["user"]
                + ":"
                + config["sql_user_key"]
                + "@"
                + config["sql_host"]
                + "/"
                + config["sql_database"]
            )
        else:
            # SQLite is raising ugly error messages when the database directory does not exist.
            if config["sql_file"] is None:
                if len(config["resource_paths"]) >= 1:
                    config["sql_file"] = "/".join(
                        [config["resource_paths"][0], "pyiron.db"]
                    )
                else:
                    config["sql_file"] = "/".join(
                        ["~", "pyiron.db"]
                    )
            sql_file = convert_path(path=config["sql_file"])
            if os.path.dirname(
                sql_file
            ) != "" and not os.path.exists(
                os.path.dirname(sql_file)
            ):
                os.makedirs(os.path.dirname(sql_file))
            config[
                "sql_connection_string"
            ] = "sqlite:///" + sql_file.replace("\\", "/")
        return config

    def _read_external_config(self, config):
        if isinstance(config, dict):
            for key, value in config.items():
                if key not in ["resource_paths", "project_paths"] or isinstance(
                    value, list
                ):
                    self._configuration[key] = value
                elif isinstance(value, str):
                    self._configuration[key] = [value]
                else:
                    TypeError(
                        "Config dictionary parameter type not recognized ", key, value
                    )

    @staticmethod
    def _get_config_from_environment(environment, config):
        env_key_mapping = {
            "PYIRONUSER": "user",
            "PYIRONRESOURCEPATHS": "resource_paths",
            "PYIRONPROJECTPATHS": "project_paths",
            "PYIRONSQLCONNECTIONSTRING": "sql_connection_string",
            "PYIRONSQLTABLENAME": "sql_table_name",
            "PYIRONSQLVIEWCONNECTIONSTRING": "sql_view_connection_string",
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
        for k, v in env_key_mapping.items():
            if k in environment.keys():
                if k in ["PYIRONPROJECTCHECKENABLED", "PYIRONDISABLE"]:
                    config[v] = environment[k].lower() in ['t', 'true', 'y', 'yes']
                elif k in ["PYIRONRESOURCEPATHS", "PYIRONPROJECTPATHS"]:
                    config[v] = environment[k].replace(",", ":").replace(";", ":").split(':')
                else:
                    config[v] = environment[k]
        return config

    @property
    def publication(self):
        return {
            "pyiron": {
                "pyiron-paper": {
                    "author": [
                        "Jan Janssen",
                        "Sudarsan Surendralal",
                        "Yury Lysogorskiy",
                        "Mira Todorova",
                        "Tilmann Hickel",
                        "Ralf Drautz",
                        "Jörg Neugebauer",
                    ],
                    "title": "pyiron: An integrated development environment for computational "
                    "materials science",
                    "journal": "Computational Materials Science",
                    "volume": "161",
                    "pages": "24 - 36",
                    "issn": "0927-0256",
                    "doi": "https://doi.org/10.1016/j.commatsci.2018.07.043",
                    "url": "http://www.sciencedirect.com/science/article/pii/S0927025618304786",
                    "year": "2019",
                }
            }
        }


def convert_path(path):
    """
    Convert path to POSIX path

    Args:
        path(str): input path

    Returns:
        str: absolute path in POSIX format
    """
    return os.path.abspath(os.path.expanduser(path)).replace("\\", "/")
