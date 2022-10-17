# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
A parent class for managing input to pyiron jobs.
"""

from abc import ABC, ABCMeta
from typing import Optional, TYPE_CHECKING

from traitlets import HasTraits, MetaHasTraits

from pyiron_base.interfaces.object import HasStorage

if TYPE_CHECKING:
    from pyiron_base.storage.hdfio import ProjectHDFio

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2022, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.1"
__maintainer__ = "Liam Huber"
__email__ = "liamhuber@greyhavensolutions.com"
__status__ = "development"
__date__ = "Oct 17, 2022"


class ABCTraitsMeta(MetaHasTraits, ABCMeta):
    """
    Just a bookkeeping necessity for classes that inherit from both `ABC` and `HasTraits`.
    """
    pass


class Input(HasTraits, HasStorage, ABC, metaclass=ABCTraitsMeta):
    """
    A base class for input to pyiron jobs, combining pyiron's `HasStorage` and `traitlets.HasTraits` for ease of access
    and validation/callbacks.

    Attributes:
        locked (bool): Whether the traits are allowed to be updated, e.g. after running a job you may want
            `job.input.lock()` to prevent the input from being changed post-facto. (Default is False.)

    TODO: In-depth docstring with examples.

    Note:
        If you write `__init__` in any child class, be sure to pass
        `super().__init__(*args, group_name=group_name, **kwargs)` to ensure that the group name for `HasStorage` gets
        set, at the trait values (if any are set during instantiation) get set.
    """

    def __init__(self, *args, group_name='input', **kwargs):
        """
        Make a new input container.

        Args:
            group_name:
        """
        super().__init__(*args, group_name=group_name, **kwargs)
        self.storage.locked = False

    @property
    def locked(self):
        return self.storage.locked

    def to_hdf(
            self,
            hdf: ProjectHDFio,
            group_name: Optional[str] = None
    ):
        for k in self.traits().keys():
            setattr(self.storage, k, getattr(self, k))
        super().to_hdf(hdf, group_name=group_name)

    def from_hdf(
            self,
            hdf: ProjectHDFio,
            group_name: Optional[str] = None
    ):
        for k, v in self.storage.items():
            setattr(self, k, v)
        super().from_hdf(hdf, group_name=group_name)

    def lock(self):
        self.storage.locked = True

    def unlock(self):
        self.storage.locked = False

    def __setattr__(self, key, value):
        if self.locked and key in self.traits().keys():
            raise RuntimeError(
                f"{self.__class__.__name__} is locked, so the trait {key} cannot be updated to {value}. Call "
                f"`.unlock()` first if you're sure you know what you're doing."
            )
        super().__setattr__(key, value)
