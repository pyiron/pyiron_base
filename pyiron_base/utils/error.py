# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
In order to be accessible from anywhere in pyiron, they *must* remain free of any imports from pyiron!
"""

from itertools import count
import time
from typing import Callable, TypeVar, Type, Tuple, Optional, Union

from pyiron_snippets.logger import logger

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


T = TypeVar("T")


def retry(
    func: Callable[[], T],
    error: Union[Type[Exception], Tuple[Type[Exception], ...]],
    msg: str,
    at_most: Optional[int] = None,
    delay: float = 1.0,
    delay_factor: float = 1.0,
) -> T:
    """
    Try to call `func` until it no longer raises `error`.

    Any other exception besides `error` is still raised.

    Args:
        func (callable): function to call, should take no arguments
        error (Exception or tuple thereof): any exceptions to be caught
        msg (str): messing to be written to the log if `error` occurs.
        at_most (int, optional): retry at most this many times, None means retry
                                forever
        delay (float): time to wait between retries in seconds
        delay_factor (float): multiply `delay` between retries by this factor

    Raises:
        `error`: if `at_most` is exceeded the last error is re-raised
        Exception: any exception raised by `func` that does not match `error`

    Returns:
        object: whatever is returned by `func`
    """
    if at_most is None:
        tries = count()
    else:
        tries = range(at_most)
    for i in tries:
        try:
            return func()
        except error as e:
            logger.warn(
                f"{msg} Trying again in {delay}s. Tried {i + 1} times so far..."
            )
            time.sleep(delay)
            delay *= delay_factor
            # e drops out of the namespace after the except clause ends, so
            # assign it here to a dummy variable so that we can re-raise it
            # in case the error persists
            err = e
    raise err from None
