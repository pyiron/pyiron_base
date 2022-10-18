# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
A parent class for managing input to pyiron jobs.
"""

from __future__ import annotations

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
        read_only (bool): Whether the traits (recursively in case and traits are also of this class) are allowed to be
            updated or are read-only. This attribute is itself read-only, but can be updated with the `lock()` and
            `unlock()` methods. (Default is False, allow both reading and writing of traits.)

    TODO: In-depth docstring with examples.

    Note:
        If you write `__init__` in any child class, be sure to pass
        `super().__init__(*args, group_name=group_name, **kwargs)` to ensure that the group name for `HasStorage` gets
        set, at the trait values (if any are set during instantiation) get set.
    """

    def setup_instance(*args, **kwargs):
        """
        This is called **before** self.__init__ is called.

        Overrides `HasTraits.setup_instance`, which gets called in `HasTraits.__new__` and initializes instances of the
        traits on self. Since we override `__setattr__` to depend on the attribute `_read_only`, we need to make sure
        this is the very first attribute that gets set!
        """
        self = args[0]
        self._read_only = False
        super(Input, self).setup_instance(*args, **kwargs)

    @property
    def read_only(self) -> bool:
        return self._read_only

    def _to_hdf(self, hdf: ProjectHDFio):
        self.storage.is_read_only = self._read_only  # read_only and _read_only are already used on DataContainer
        for k in self.traits().keys():
            setattr(self.storage, k, getattr(self, k))
        super()._to_hdf(hdf)

    def _from_hdf(self, hdf: ProjectHDFio, version: Optional[str] = None):
        super()._from_hdf(hdf, version=version)
        if len(self.storage) > 0:
            read_only = self.storage.pop('is_read_only')
            for k, v in self.storage.items():
                setattr(self, k, v)
            self._read_only = read_only

    def lock(self):
        """Recursively make all traits read-only."""
        self._read_only = True
        for sub in self.trait_values().values():
            try:
                sub.lock()
            except AttributeError:
                pass

    def unlock(self):
        """Recursively make all traits both readable and writeable"""
        self._read_only = False
        for sub in self.trait_values().values():
            try:
                sub.unlock()
            except AttributeError:
                pass

    def __setattr__(self, key, value):
        if key == '_read_only':
            super(Input, self).__setattr__(key, value)
        elif self.read_only and key in self.traits().keys():
            raise RuntimeError(
                f"{self.__class__.__name__} is locked, so the trait {key} cannot be updated to {value}. Call "
                f"`.unlock()` first if you're sure you know what you're doing."
            )
        super().__setattr__(key, value)
