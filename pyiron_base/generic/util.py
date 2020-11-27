# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
"""

import functools
import types
import warnings

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


class Singleton(type):
    """
    Implemented with suggestions from

    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def static_isinstance(obj, obj_type):
    """
    A static implementation of isinstance() - instead of comparing an object and a class, the object is compared to a
    string, like 'pyiron_base.job.generic.GenericJob' or a list of strings.

    Args:
        obj: the object to check
        obj_type (str/list): object type as string or a list of object types as string.

    Returns:
        bool: [True/False]
    """
    if not hasattr(obj, "__mro__"):
        obj = obj.__class__
    obj_class_lst = [
        ".".join([subcls.__module__, subcls.__name__]) for subcls in obj.__mro__
    ]
    if isinstance(obj_type, list):
        return any([obj_type_element in obj_class_lst for obj_type_element in obj_type])
    elif isinstance(obj_type, str):
        return obj_type in obj_class_lst
    else:
        raise TypeError()


class Deprecator:
    """
    Decorator class to mark functions and methods as deprecated with a uniform
    warning message at the time the function is called.  The message has the
    form

        {function_name} is deprecated: {message}. It is not guaranteed to be in
        service after {version}.

    unless `pending=True` was given.  Then the message will be

        {function_name} will be deprecated in a future version: {message}.

    If message and version are not initialized or given during the decorating
    call the respective parts are left out from the message.

    >>> deprecate = Deprecator()
    >>> @deprecate
    ... def foo(a, b):
    ...     pass
    >>> foo(1, 2)
    DeprecationWarning: __main__.foo is deprecated

    >>> @deprecate("use bar() instead")
    ... def foo(a, b):
    ...     pass
    >>> foo(1, 2)
    DeprecationWarning: __main__.foo is deprecated: use bar instead

    >>> @deprecate("use bar() instead", version="0.4.0")
    ... def foo(a, b):
    ...     pass
    >>> foo(1, 2)
    DeprecationWarning: __main__.foo is deprecated: use bar instead.  It is not
    guaranteed to be in service in vers. 0.4.0

    >>> deprecate = Deprecator(message="pyiron says no!", version="0.5.0")
    >>> @deprecate
    ... def foo(a, b):
    ...     pass
    >>> foo(1, 2)
    DeprecationWarning: __main__.foo is deprecated: pyiron says no!  It is not
    guaranteed to be in service in vers. 0.5.0
    """

    def __init__(self, message=None, version=None, pending=False):
        """
        Initialize default values for deprecation message and version.

        Args:
            message (str): default deprecation message
            version (str): default version after which the function might be removed
            pending (bool): only warn about future deprecation, warning category will be PendingDeprecationWarning
                instead of DeprecationWarning
        """
        self.message = message
        self.version = version
        self.category = PendingDeprecationWarning if pending else DeprecationWarning

    def __call__(self, message, version = None):
        if isinstance(message, types.FunctionType):
            return self.wrap(message)
        else:
            self.message = message
            self.version = version
            return self.wrap

    def wrap(self, function):
        """
        Wrap the given function to emit a DeprecationWarning at call time.  The warning message is constructed from the
        given message and version.

        Args:
            function (function): function to mark as deprecated

        Return:
            function: raises DeprecationWarning when given function is called
        """
        if self.category == PendingDeprecationWarning:
            message_format =  "{}.{} is deprecated"
        else:
            message_format =  "{}.{} will be deprecated"
        message = message_format.format(function.__module__, function.__name__)

        if self.message is not None:
            message += ": {}.".format(self.message)
        else:
            message += "."

        if self.version is not None:
            message += " It is not guaranteed to be in service in vers. {}".format(self.version)

        @functools.wraps(function)
        def decorated(*args, **kwargs):
            warnings.warn(
                message,
                category=self.category,
                stacklevel=2
            )
            return function(*args, **kwargs)
        return decorated

deprecate = Deprecator()
deprecate_soon = Deprecator(pending=True)
