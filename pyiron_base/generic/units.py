# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import functools
import numpy as np

__author__ = "Sudarsan Surendralal"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)


class UnitsBase:
    """
    Module to record units for physical quantities within pyiron

    """
    def __init__(self):
        self._quantity_dict = dict()
        self._dtype_dict = dict()
        self._unit_dict = dict()

    @property
    def quantity_dict(self):
        return self._quantity_dict

    @property
    def dtype_dict(self):
        return self._dtype_dict

    @property
    def unit_dict(self):
        return self._unit_dict

    def add_quantity(self, quantity, unit, data_type=float):
        self._unit_dict[quantity] = unit
        self._dtype_dict[quantity] = data_type

    def add_labels(self, labels, quantity):
        for label in labels:
            self._quantity_dict[label] = quantity

    def __getitem__(self, item):
        if item in self._unit_dict.keys():
            return self._unit_dict[item]
        elif item in self._quantity_dict.keys():
            return self._unit_dict[self._quantity_dict[item]]

    def get_dtype(self, item):
        if item in self._unit_dict.keys():
            return self._dtype_dict[item]
        elif item in self._quantity_dict.keys():
            return self._dtype_dict[self._quantity_dict[item]]


class UnitConverter:

    def __init__(self, base_units, code_units):
        self._base_units = base_units
        self._code_units = code_units

    def code_to_base(self, quantity):
        return (1 * self._code_units[quantity]).to(self._base_units[quantity])

    def base_to_code(self, quantity):
        return (1 * self._base_units[quantity]).to(self._code_units[quantity])

    def code_to_base_value(self, quantity):
        return self.code_to_base(quantity).magnitude

    def base_to_code_value(self, quantity):
        return self.base_to_code(quantity).magnitude

    def __call__(self, conversion, quantity):
        if conversion == "to_base":
            def __decorate_to_base(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(function(*args, **kwargs) * self.code_to_base_value(quantity),
                                    dtype=self._base_units.get_dtype(quantity))
                return dec
            return __decorate_to_base
        elif conversion == "to_code":
            def __decorate_to_code(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(function(*args, **kwargs) * self.base_to_code_value(quantity),
                                    dtype=self._base_units.get_dtype(quantity))
                return dec
            return __decorate_to_code
        else:
            raise ValueError()
