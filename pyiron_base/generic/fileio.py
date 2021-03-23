"""
Functions for reading and writing data files.
"""
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import ast
from collections import namedtuple
import os.path
import yaml
import warnings


__author__ = "Muhammad Hassani, Marvin Poul"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Muhammad Hassani"
__email__ = "hassani@mpie.de"
__status__ = "production"
__date__ = "Feb 26, 2021"


def read(file_name):
    """
    Read data from a file.

    Format is determined from the file extension as follows
    - yaml: .yaml, .yml

    Args:
        file_name (str): file name, extension defines which format is used

    Returns:
        nested dict/list

    Raises:
        ValueError: if given filename doesn't have one of the specified extensions
    """
    return _lookup_adapter(file_name).parse(file_name)


def write(data, file_name):
    """
    Writes the data to a file.

    Format is determined from the file extension as follows
    - yaml: .yaml, .yml

    Args:
        data (nested dict/list): data to save to file, dictionary keys must be str!
        file_name (str): file name, extension defines which format is used

    Raises:
        ValueError: if given filename doesn't have one of the specified extensions
    """
    return _lookup_adapter(file_name).write(data, file_name)


def _lookup_adapter(file_name):
    """Find adapter for filename based on its extensions."""
    ext = os.path.splitext(file_name)[1][1:]
    try:
        return _FILETYPE_DICT[ext]
    except KeyError:
        raise ValueError("The file type is not supported; expected *.yml, *.yaml,  not \"{}\"".format(ext)) \
                from None


def _parse_yml(file_name):
    """
    Parse a YAML file as a dict.  Errors during reading raise
    a warning and return an empty dict.

    Args:
        file_name(str): path to the input file; it should be a YAML file.

    Returns:
        dict: parsed file contents
    """
    with open(file_name, 'r') as input_src:
        try:
            return yaml.safe_load(input_src)
        except yaml.YAMLError as exc:
            warnings.warn(exc)
            return {}


def _to_yml(data, file_name):
    """
    Writes the DataContainer to a yaml file.

    Args:
        data (nested dict/list): data to save to file, dictionary keys must be str!
        file_name (str): the name of the file to be writen to.
    """
    with open(file_name, 'w') as output:
        yaml.dump(data, output, default_flow_style=False)


FileAdapter = namedtuple('FileAdapter', ('parse', 'write'))
YMLAdapter = FileAdapter(_parse_yml, _to_yml)

_FILETYPE_DICT = {
        'yaml': YMLAdapter,
        'yml': YMLAdapter
}
