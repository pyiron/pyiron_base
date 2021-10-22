# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
A helper class to give quick and easy access to all the singleton classes which together define the IDE.
"""

from pyiron_base.generic.singleton import Singleton
from pyiron_base.ide.settings import Settings
from pyiron_base.database.manager import DatabaseManager

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Oct 22, 2021"

s = Settings()
dbm = DatabaseManager()


class IDE(metaclass=Singleton):
    @property
    def s(self) -> Settings:
        return s

    @property
    def dbm(self) -> DatabaseManager:
        return dbm

    @property
    def logger(self):
        return s.logger

    @property
    def queue_adapter(self):
        return s.queue_adapter


ide = IDE()
