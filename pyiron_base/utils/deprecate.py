# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
In order to be accessible from anywhere in pyiron, they *must* remain free of any imports from pyiron!
"""
from copy import copy
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

    Alternatively the decorator can also be called with `arguments` set to a dictionary mapping names of keyword
    arguments to deprecation messages.  In this case the warning will only be emitted when the decorated function is
    called with arguments in that dictionary.

    >>> deprecate = Deprecator()
    >>> @deprecate(arguments={"bar": "use baz instead."})
    ... def foo(bar=None, baz=None):
    ...     pass
    >>> foo(baz=True)
    >>> foo(bar=True)
    DeprecationWarning: __main__.foo(bar=True) is deprecated: use baz instead.

    As a short cut, it is also possible to pass the values in the arguments dict directly as keyword arguments to the
    decorator.

    >>> @deprecate(bar="use baz instead.")
    ... def foo(bar=None, baz=None):
    ...     pass
    >>> foo(baz=True)
    >>> foo(bar=True)
    DeprecationWarning: __main__.foo(bar=True)  is deprecated: use baz instead.
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

    def __copy__(self):
        cp = type(self)(message=self.message, version=self.version)
        cp.category = self.category
        return cp

    def __call__(self, message=None, version=None, arguments=None, **kwargs):
        depr = copy(self)
        if isinstance(message, types.FunctionType):
            return depr.__deprecate_function(message)
        else:
            depr.message = message
            depr.version = version
            depr.arguments = arguments if arguments is not None else {}
            depr.arguments.update(kwargs)
            return depr.wrap

    def _build_message(self):
        if self.category == PendingDeprecationWarning:
            message_format = "{} will be deprecated"
        else:
            message_format = "{} is deprecated"

        if self.message is not None:
            message_format += ": {}.".format(self.message)
        else:
            message_format += "."

        if self.version is not None:
            message_format += (
                " It is not guaranteed to be in service in vers. {}".format(
                    self.version
                )
            )

        return message_format

    def __deprecate_function(self, function):
        message = self._build_message().format(
            "{}.{}".format(function.__module__, function.__name__)
        )

        @functools.wraps(function)
        def decorated(*args, **kwargs):
            warnings.warn(message, category=self.category, stacklevel=2)
            return function(*args, **kwargs)

        return decorated

    def __deprecate_argument(self, function):
        message_format = self._build_message()

        @functools.wraps(function)
        def decorated(*args, **kwargs):
            for kw in kwargs:
                if kw in self.arguments:
                    warnings.warn(
                        message_format.format(
                            "{}.{}({}={})".format(
                                function.__module__, function.__name__, kw, kwargs[kw]
                            )
                        ),
                        category=self.category,
                        stacklevel=2,
                    )
            return function(*args, **kwargs)

        return decorated

    def wrap(self, function):
        """
        Wrap the given function to emit a DeprecationWarning at call time.  The warning message is constructed from the
        given message and version.  If :attr:`.arguments` is set then the warning is only emitted, when the decorated
        function is called with keyword arguments found in that dictionary.

        Args:
            function (function): function to mark as deprecated

        Return:
            function: raises DeprecationWarning when given function is called
        """
        if not self.arguments:
            return self.__deprecate_function(function)
        else:
            return self.__deprecate_argument(function)


deprecate = Deprecator()
deprecate_soon = Deprecator(pending=True)
