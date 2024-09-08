# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Utility functions used in pyiron.
In order to be accessible from anywhere in pyiron, they *must* remain free of any imports from pyiron!
"""

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


import importlib
from typing import List, Union


def static_isinstance(obj: object, obj_type: Union[str, List[str]]) -> bool:
    """
    A static implementation of isinstance() - instead of comparing an object and a class, the object is compared to a
    string, like 'pyiron_base.jobs.job.generic.GenericJob' or a list of strings.

    Args:
        obj (object): The object to check.
        obj_type (str/list): Object type as string or a list of object types as string.

    Returns:
        bool: True if the object is an instance of any of the specified types, False otherwise.
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


def import_class(class_type: str) -> type:
    """
    Import a class dynamically based on its fully qualified name.

    Args:
        class_type (str): The fully qualified name of the class, e.g. 'module.submodule.ClassName'.

    Returns:
        type: The imported class.

    Raises:
        ImportError: If the module or class cannot be imported.
        AttributeError: If the class does not exist in the module.
    """
    module_path, class_name = class_type.rsplit(".", maxsplit=1)
    return getattr(
        importlib.import_module(module_path),
        class_name,
    )
