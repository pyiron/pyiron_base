# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The `state` module holds (almost!) all the code for defining the global state of a pyiron instance.
Such "global" behaviour is achieved by using the `Singleton` metaclass to guarantee that each class only even has a
single instance per session.
These are all instantiated for the first time inside their respective module for easy access, and collected here in the
init under the `state` object to give a single, even easier point of access.

Here's the "almost": Right now the database management still lives off in its own module but is referenced here and
ultimately should probably be relocated here (work is ongoing on our database interaction...), and there is a
`JobTypeChoice` class that is anyhow on the chopping block and will be deleted once we don't need it for backwards
compatibility.
"""

from typing import Dict, Union

from pyiron_snippets.logger import logger as _logger
from pyiron_snippets.singleton import Singleton

from pyiron_base.database.manager import database as _database
from pyiron_base.state.publications import publications as _publications
from pyiron_base.state.queue_adapter import queue_adapters as _queue_adapters
from pyiron_base.state.settings import settings as _settings

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Oct 22, 2021"


class State(metaclass=Singleton):
    """
    A helper class to give quick and easy access to all the singleton classes which together define the state module.

    Attributes:
        logger: Self-explanatory.
        publications: Bibliography of papers which should be cited based on the code that was used (alpha feature).
        settings: System settings.
        database: Database (or file base) connection.
        queue_adapter: Configuration for using remote resources.
    """

    @property
    def logger(self):
        return _logger

    @property
    def publications(self):
        return _publications

    @property
    def settings(self):
        return _settings

    @property
    def database(self):
        return _database

    @property
    def queue_adapter(self):
        return _queue_adapters.adapter

    def update(self, config_dict: Union[Dict, None] = None) -> None:
        """
        Re-reads the settings configuration, then reconstructs the queue adapter and reboots the database connection.

        Args:
            config_dict (dict): A new set of configuration parameters to use. (Default is None, which attempts to read
                the configuration from system environment xor configuration files.)
        """
        self.settings.update(user_dict=config_dict)
        _queue_adapters.update()
        self.database.update()


state = State()
