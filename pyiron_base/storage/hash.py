# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Create hash for a given dictionary
"""

import numpy as np
from typing import Dict, Any
import hashlib
import json

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


def dict_hash(dictionary: Dict[str, Any], sort_keys=True) -> str:
    """
    Return hash in hexdecimal digits

    Args:
        dictionary (dict): dict for which hash is to be created
        sort_keys (bool): whether or not sort keys

    Returns:
        (str): hash in hexdecimal digits
    """
    dhash = hashlib.md5()
    encoded = json.dumps(dictionary, sort_keys=sort_keys).encode()
    dhash.update(encoded)
    return dhash.hexdigest()
