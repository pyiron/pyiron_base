# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import functools
import numpy as np
import pint

__author__ = "Sudarsan Surendralal"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)


class PyironUnitRegistry:
    """
    Module to record units for physical quantities within pyiron. This module is used for defining the units
    for different pyiron submodules.

    Useage:

    >>> import pint
    >>> from pyiron_base.generic.units import PyironUnitRegistry
    >>> pint_registry = pint.UnitRegistry()

    After instantiating, the `pint` units for different physical quantities can be registered as follows

    >>> base_units = PyironUnitRegistry()
    >>> base_units.add_quantity(quantity="energy", unit=pint_registry.eV, data_type=float)

    Labels corresponding to a particular physical quantity can also be registered

    >>> base_units.add_labels(labels=["energy_tot", "energy_pot"], quantity="energy")

    For more information on working with `pint`, see: https://pint.readthedocs.io/en/0.10.1/tutorial.html
    """
    def __init__(self):
        self._quantity_dict = dict()
        self._dtype_dict = dict()
        self._unit_dict = dict()

    @property
    def quantity_dict(self):
        """
        A dictionary of the different labels stored and the physical quantity they correspond to

        Returns:
            dict
        """
        return self._quantity_dict

    @property
    def dtype_dict(self):
        """
        A dictionary of the different physical quantities and the corresponding datatype in which they are to be stored

        Returns:
            dict
        """
        return self._dtype_dict

    @property
    def unit_dict(self):
        """
        A dictionary of the different physical quantities and the corresponding `pint` unit

        Returns:
            dict
        """
        return self._unit_dict

    def add_quantity(self, quantity, unit, data_type=float):
        """
        Add a quantity to a registry

        Args:
            quantity (str): The physical quantity
            unit (pint.unit.Unit/pint.quantity.Quantity): `pint` unit or quantity
            data_type (type): Data type in which the quantity has to be stored

        """
        if not isinstance(unit, (pint.unit.Unit, pint.quantity.Quantity)):
            raise ValueError("The unit should be a `pint` unit or quantity")
        self._unit_dict[quantity] = unit
        self._dtype_dict[quantity] = data_type

    def add_labels(self, labels, quantity):
        """
        Maps quantities with different labels to quantities already defined in the registry

        Args:
            labels (list/ndarray): List of labels
            quantity (str): Physical quantity associated with the labels

        Note: `quantity` should already be a key of unit_dict

        """
        for label in labels:
            if quantity in self.unit_dict.keys():
                self._quantity_dict[label] = quantity
            else:
                raise ValueError("Quantity {} is not defined. Use `add_quantity` to register the unit of this label")

    def __getitem__(self, item):
        """
        Getter to return corresponding `pint` unit for a quantity

        Args:
            item (str):

        Returns:
            pint.unit.Unit/pint.quantity.Quantity: The corresponding `pint` unit/quantity
        """
        if item in self._unit_dict.keys():
            return self._unit_dict[item]
        elif item in self._quantity_dict.keys():
            return self._unit_dict[self._quantity_dict[item]]
        else:
            raise ValueError("Quantity/label '{}' not registered in this unit registry".format(item))

    def get_dtype(self, quantity):
        """
        Returns the data type in which the quantity will be stored

        Args:
            quantity (str): The quantity

        Returns:
            type: Corresponding data type
        """
        if quantity in self._unit_dict.keys():
            return self._dtype_dict[quantity]
        elif quantity in self._quantity_dict.keys():
            return self._dtype_dict[self._quantity_dict[quantity]]
        else:
            raise ValueError("Quantity/label '{}' not registered in this unit registry".format(quantity))


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
            def _decorate_to_base(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(function(*args, **kwargs) * self.code_to_base_value(quantity),
                                    dtype=self._base_units.get_dtype(quantity))
                return dec
            return _decorate_to_base
        elif conversion == "to_code":
            def _decorate_to_code(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(function(*args, **kwargs) * self.base_to_code_value(quantity),
                                    dtype=self._base_units.get_dtype(quantity))
                return dec
            return _decorate_to_code
        else:
            raise ValueError()
