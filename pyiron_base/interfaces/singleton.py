# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
In order to be accessible from anywhere in pyiron, they *must* remain free of any imports from pyiron!
"""
from abc import ABCMeta

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Singleton(ABCMeta):
    """
    Implemented with suggestions from

    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class UpdatableSingleton(ABCMeta):
    """
    Like the `Singeton` class, but will re-run `__init__` on each instantiation.

    This means that subsequent reinitializations have access to the existing state of
    the object, against which comparisons can be made.

    Examples)
    >>> class Foo(metaclass=UpdatableSingleton):
    ...      def __init__(self, x=None):
    ...         try:
    ...             if x is not None and x != self.x:
    ...                 self.x = x
    ...                 self.y = self.compute_y(x)
    ...         except AttributeError:
    ...             self.x = x
    ...             self.y = self.compute_y(x)
    ...
    ...     def compute_y(self, x):
    ...         # Imagine this was expensive and we wanted to avoid it
    ...         return x**2 if x is not None else None
    >>>
    >>> foo = Foo(5)
    >>> print(foo.x, foo.y)
    ... 5 25
    >>> bar = Foo(6)
    >>> print(foo.x, foo.y, bar.x, bar.y)
    ... 6 36 6 36
    >>> # A singleton, but with updated values!
    >>> baz = Foo()
    >>> print(foo.x, foo.y, bar.x, bar.y)
    ... 6 36
    >>> # And we wrote Foo so it was easy to recover the object with its existing state
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(UpdatableSingleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]
