"""
Mixin for classes that represent a hierarchical structures of "groups" and "nodes".
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from abc import ABC, abstractmethod

__author__ = "Marvin Poul"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Marvin Poul"
__email__ = "poul@mpie.de"
__status__ = "production"
__date__ = "May 31, 2021"


class HasGroups(ABC):
    """
    Abstract mixin to capture hierarchical structure of pyiron objects.

    Necessary overrides are :method:`.__getitem__()`, :method:`._list_groups()` and :method:`._list_nodes()`.  Sub
    classes may override :method:`._list_all()`, but the default implementation will call the other two methods.

    A hierarchical object has a number of children and associates them with string names.  :method:`.__getitem__()`
    looks up a child with the given name.  Some of these children may have children of their own and also implement
    :class:`.HasGroups`.  These are called "groups"; other children are "nodes".  :method:`._list_groups()` and
    :method:`._list_nodes()` must return the names of the respective type of children.

    Sub classes should document in their class docstring what they consider has "groups" or "nodes".

    Here's an example class that uses nested dicts to store children

    >>> class NestedDicts(HasGroups):
    ...
    ...    def __init__(self, data):
    ...        self._data = data
    ...
    ...    def __getitem__(self, key):
    ...        v = self._data[key]
    ...        if isinstance(v, dict):
    ...            return NestedDicts(v)
    ...        else:
    ...            return v
    ...
    ...    def _list_groups(self):
    ...        return [k for k, v in self._data.items() if isinstance(v, dict)]
    ...
    ...    def _list_nodes(self):
    ...        return [k for k, v in self._data.items() if not isinstance(v, dict)]
    >>> nd = NestedDicts({"foo": 0, "bar": 1, "baz": {"apple": "yummy", "pear": "yuck"}})
    >>> nd.list_nodes()
    ['foo', 'bar']
    >>> nd.list_groups()
    ['baz']
    >>> isinstance(nd["baz"], HasGroups)
    True
    """

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def _list_groups(self):
        """
        Return a list of names of all nested groups.

        Indexing the object with names returned from here, must return instances of :class:`.HasGroups`.

        Returns:
            list of str: group names
        """
        pass

    @abstractmethod
    def _list_nodes(self):
        """
        Return a list of names of all nested nodes.

        Indexing the object with names returned from here may return any type at all, but names must not overlap with
        names returned from :method:`._list_groups()`.

        Returns:
            list of str: node names
        """
        pass

    def _list_all(self):
        return {"groups": self._list_groups(), "nodes": self._list_nodes()}

    def list_groups(self):
        """
        Return a list of names of all nested groups.

        Returns:
            list of str: group names
        """
        return self._list_groups()

    def list_nodes(self):
        """
        Return a list of names of all nested nodes.

        Returns:
            list of str: node names
        """
        return self._list_nodes()

    def list_all(self):
        """
        Returns dictionary of :method:`.list_groups()` and :method:`.list_nodes()`.

        Returns:
            dict: results of :method:`.list_groups() under the key "groups"; results of :method:`.list_nodes()` und the
                  key "nodes"
        """
        return self._list_all()
