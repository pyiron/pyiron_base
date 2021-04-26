"""
Mixin for classes that represent a hierachial structures of "groups" and "nodes".
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from abc import ABC, abstractmethod

class HasGroups(ABC):

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def _list_groups(self):
        pass

    @abstractmethod
    def _list_nodes(self):
        pass

    def _list_all(self):
        return {"groups": self._list_groups(), "nodes": self._list_nodes()}

    def list_groups(self):
        """
        Returns:
            list: 
        """
        return self._list_groups()

    def list_nodes(self):
        """
        """
        return self._list_nodes()

    def list_all(self):
        """
        """
        return self._list_all()
