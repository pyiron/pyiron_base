# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
In order to be accessible from anywhere in pyiron, they *must* remain free of any imports from pyiron!
"""
import functools
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


class ImportAlarm:
    """
    In many places we have try/except loops around imports. This class is meant to accompany that code so that users
    get an early warning when they instantiate a job that won't work when run.

    Example:

    >>> try:
    ...     from mystery_package import Enigma, Puzzle, Conundrum
    ...     import_alarm = ImportAlarm()
    >>> except ImportError:
    >>>     import_alarm = ImportAlarm(
    ...         "MysteryJob relies on mystery_package, but this was unavailable. Please ensure your python environment "
    ...         "has access to mystery_package, e.g. with `conda install -c conda-forge mystery_package`"
    ...     )
    ...
    >>> class MysteryJob(GenericJob):
    ...     @import_alarm
    ...     def __init__(self, project, job_name)
    ...         super().__init__()
    ...         self.riddles = [Enigma(), Puzzle(), Conundrum()]

    This class is also a context manager that can be used as a short-cut, like this:

    >>> with ImportAlarm("MysteryJob relies on mystery_package, but this was unavailable.") as import_alarm:
    ...     import mystery_package

    If you do not use `import_alarm` as a decorator, but only to get a consistent warning message, call
    :meth:`.warn_if_failed()` after the with statement.

    >>> import_alarm.warn_if_failed()
    """

    def __init__(self, message=None):
        """
        Initialize message value.

        Args:
            message (str): What to say alongside your ImportError when the decorated function is called. (Default is
                None, which says nothing and raises no error.)
        """
        self.message = message

    def __call__(self, func):
        return self.wrapper(func)

    def wrapper(self, function):
        @functools.wraps(function)
        def decorator(*args, **kwargs):
            self.warn_if_failed()
            return function(*args, **kwargs)

        return decorator

    def warn_if_failed(self):
        """
        Print warning message if import has failed.  In case you are not using `ImportAlarm` as a decorator you can call
        this method manually to trigger the warning.
        """
        if self.message is not None:
            warnings.warn(self.message, category=ImportWarning)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type == exc_value == traceback == None:
            # import successful, so silence our warning
            self.message = None
            return
        if issubclass(exc_type, ImportError):
            # import broken; retain message, but suppress error
            return True
        else:
            # unrelated error during import, re-raise
            return False
