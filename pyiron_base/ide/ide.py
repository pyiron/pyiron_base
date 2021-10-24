# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
A helper class to give quick and easy access to all the singleton classes which together define the IDE.
"""

from pyiron_base.generic.singleton import Singleton
from pyiron_base.ide.settings import settings
from pyiron_base.database.manager import database
from pyiron_base.ide.logger import logger

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


class IDE(metaclass=Singleton):
    # With python >=3.9 we can just use @classmethod and @property together so these can be safe from being overwritten
    # But with earlier versions the implementation is ugly, so live dangerously
    # https://stackoverflow.com/questions/128573/using-property-on-classmethods
    settings = settings
    database = database
    logger = logger
    queue_adapter = settings.queue_adapter
