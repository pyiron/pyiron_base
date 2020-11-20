# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Abstract classes for factories that instantiate other pyiron objects.
"""

from abc import ABC

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.1"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Nov 20, 2020"


class PyironFactory(ABC):
    """
    A base class for factories, an abstraction layer which help facilitate tab-completion.
    """

    def __getitem__(self, key):
        return getattr(self, key)
