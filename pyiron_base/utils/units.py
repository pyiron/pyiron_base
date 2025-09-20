# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import functools
from typing import Union

import numpy as np
import pint

Q_ = pint.Quantity

__author__ = "Sudarsan Surendralal"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)


class PyironUnitRegistry:
    """
    Module to record units for physical quantities within pyiron. This module is used for defining the units
    for different pyiron submodules.

    Usage:

    >>> import pint
    >>> from pyiron_base.utils.units import PyironUnitRegistry
    >>> pint_registry = pint.UnitRegistry()

    After instantiating, the `pint` units for different physical quantities can be registered as follows

    >>> base_registry = PyironUnitRegistry()
    >>> base_registry.add_quantity(quantity="energy", unit=pint_registry.eV, data_type=float)

    Labels corresponding to a particular physical quantity can also be registered

    >>> base_registry.add_labels(labels=["energy_tot", "energy_pot"], quantity="energy")

    For more information on working with `pint`, see: https://pint.readthedocs.io/en/0.10.1/tutorial.html
    """

    def __init__(self):
        """
        Attributes:
            self.quantity_dict
            self.unit_dict
            self.dtype_dict
        """
        self._quantity_dict = dict()
        self._dtype_dict = dict()
        self._unit_dict = dict()

    @property
    def quantity_dict(self) -> dict:
        """
        A dictionary of the different labels stored and the physical quantity they correspond to

        Returns:
            dict
        """
        return self._quantity_dict

    @property
    def dtype_dict(self) -> dict:
        """
        A dictionary of the names of the different physical quantities to the corresponding datatype in which they are
        to be stored

        Returns:
            dict
        """
        return self._dtype_dict

    @property
    def unit_dict(self) -> dict:
        """
        A dictionary of the different physical quantities and the corresponding `pint` unit

        Returns:
            dict
        """
        return self._unit_dict

    def add_quantity(
        self,
        quantity: str,
        unit: Union[pint.Unit, pint.Quantity],
        data_type: type = float,
    ) -> None:
        """
        Add a quantity to a registry

        Args:
            quantity (str): The physical quantity
            unit (pint.unit.Unit/pint.quantity.Quantity): `pint` unit or quantity
            data_type (type): Data type in which the quantity has to be stored

        """
        if not isinstance(unit, (pint.Unit, pint.Quantity)):
            raise ValueError("The unit should be a `pint` unit or quantity")
        self._unit_dict[quantity] = unit
        self._dtype_dict[quantity] = data_type

    def add_labels(self, labels: Union[list, np.ndarray], quantity: str) -> None:
        """
        Maps quantities with different labels to quantities already defined in the registry

        Args:
            labels (list/ndarray): List of labels
            quantity (str): Physical quantity associated with the labels

        Raises:
            KeyError: If quantity is not yet added with :method:`.add_quantity()`

        Note: `quantity` should already be a key of unit_dict

        """
        for label in labels:
            if quantity in self.unit_dict.keys():
                self._quantity_dict[label] = quantity
            else:
                raise KeyError(
                    "Quantity {} is not defined. "
                    "Use `add_quantity` to register the unit of this label".format(
                        quantity
                    )
                )

    def __getitem__(self, item: str) -> Union[pint.Unit, pint.Quantity]:
        """
        Getter to return corresponding `pint` unit for a given quantity

        Args:
            item (str):

        Returns:
            pint.unit.Unit/pint.quantity.Quantity: The corresponding `pint` unit/quantity

        Raises:
            KeyError: If quantity is not yet added with :method:`.add_quantity()` or :method:`.add_labels()`
        """
        if item in self._unit_dict.keys():
            return self._unit_dict[item]
        elif item in self._quantity_dict.keys():
            return self._unit_dict[self._quantity_dict[item]]
        else:
            raise KeyError(
                "Quantity/label '{}' not registered in this unit registry".format(item)
            )

    def get_dtype(self, quantity: str) -> type:
        """
        Returns the data type in which the quantity will be stored

        Args:
            quantity (str): The quantity

        Returns:
            type: Corresponding data type

        Raises:
            KeyError: If quantity is not yet added with :method:`.add_quantity()` or :method:`.add_labels()`
        """
        if quantity in self._unit_dict.keys():
            return self._dtype_dict[quantity]
        elif quantity in self._quantity_dict.keys():
            return self._dtype_dict[self._quantity_dict[quantity]]
        else:
            raise KeyError(
                "Quantity/label '{}' not registered in this unit registry".format(
                    quantity
                )
            )


class UnitConverter:
    """
    Module to handle conversions between two different unit registries mainly use to convert units between codes and
    pyiron submodules.

    To instantiate this class, you need two units registries: a base units registry and a code registry:

    >>> import pint
    >>> pint_registry = pint.UnitRegistry()
    >>> base = PyironUnitRegistry()
    >>> base.add_quantity(quantity="energy", unit=pint_registry.eV)
    >>> code = PyironUnitRegistry()
    >>> code.add_quantity(quantity="energy",
    ...                         unit=pint_registry.kilocal / (pint_registry.mol * pint_registry.N_A))
    >>> unit_converter = UnitConverter(base_registry=base, code_registry=code)

    The unit converter instance can then be used to obtain conversion factors between code and base units either as a
    `pint` quantity:

    >>> print(unit_converter.code_to_base_pint("energy"))
    0.043364104241800934 electron_volt

    or as a scalar:

    >>> print(unit_converter.code_to_base_value("energy"))
    0.043364104241800934

    Alternatively, the unit converter can also be used as decorators for functions that return an array scaled into
    appropriate units:

    >>> @unit_converter.code_to_base(quantity="energy")
    ... def return_ones():
    ...    return np.ones(5)
    >>> print(return_ones())
    [0.0433641 0.0433641 0.0433641 0.0433641 0.0433641]

    The decorator can also be used to assign units for numpy arrays
    (for more info see https://pint.readthedocs.io/en/0.10.1/numpy.html)

    >>> @unit_converter.base_units(quantity="energy")
    ... def return_ones_ev():
    ...     return np.ones(5)
    >>> print(return_ones_ev())
    [1.0 1.0 1.0 1.0 1.0] electron_volt

    """

    def __init__(
        self, base_registry: PyironUnitRegistry, code_registry: PyironUnitRegistry
    ):
        """
        Args:
            base_registry (PyironUnitRegistry): Base unit registry
            code_registry (PyironUnitRegistry): Code specific unit registry
        """
        self._base_registry = base_registry
        self._code_registry = code_registry
        self._check_quantities()
        self._check_dimensionality()

    def _check_quantities(self) -> None:
        base_quant = list(self._base_registry.unit_dict.keys())
        for quant in self._code_registry.unit_dict.keys():
            if quant not in base_quant:
                raise ValueError(
                    "quantity {} is not defined in the base registry".format(quant)
                )

    def _check_dimensionality(self) -> None:
        for quant in self._code_registry.unit_dict.keys():
            if (
                not self._base_registry[quant].dimensionality
                == self._code_registry[quant].dimensionality
            ):
                raise pint.DimensionalityError(
                    self._base_registry[quant],
                    self._code_registry[quant],
                    extra_msg="\n Dimensional inequality: Quantity {} has dimensionality {} "
                    "in the base registry but {} in the code "
                    "registry".format(
                        quant,
                        self._base_registry[quant].dimensionality,
                        self._code_registry[quant].dimensionality,
                    ),
                )

    def code_to_base_pint(self, quantity: str) -> pint.Quantity:
        """
        Get the conversion factor as a `pint` quantity from code to base units

        Args:
            quantity (str): Name of quantity

        Returns:
            pint.Quantity: Conversion factor as a `pint` quantity
        """
        return (1 * self._code_registry[quantity]).to(self._base_registry[quantity])

    def base_to_code_pint(self, quantity: str) -> pint.Quantity:
        """
        Get the conversion factor as a `pint` quantity from base to code units

        Args:
            quantity (str): Name of quantity

        Returns:
            pint.Quantity: Conversion factor as a `pint` quantity
        """
        return (1 * self._base_registry[quantity]).to(self._code_registry[quantity])

    def code_to_base_value(self, quantity: str) -> float:
        """
        Get the conversion factor as a scalar from code to base units

        Args:
            quantity (str): Name of quantity

        Returns:
            float: Conversion factor as a float
        """
        return self.code_to_base_pint(quantity).magnitude

    def base_to_code_value(self, quantity: str) -> float:
        """
        Get the conversion factor as a scalar from base to code units

        Args:
            quantity (str): Name of quantity

        Returns:
            float: Conversion factor as a float
        """
        return self.base_to_code_pint(quantity).magnitude

    def __call__(self, conversion: str, quantity: str) -> callable:
        """
        Function call operator used as a decorator for functions that return numpy array

        Args:
            conversion (str): Conversion type which should be one of
                'code_to_base' To multiply by the code to base units conversion factor
                'base_to_code' To multiply by the base to code units conversion factor
                'code_units' To assign code units to the nunpy array returned by the decorated function
                'base_units' To assign base units to the nunpy array returned by the decorated function

            quantity (str): Name of quantity

        Returns:
            function: Decorated function
        """
        if conversion == "code_to_base":

            def _decorate_to_base(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(
                        function(*args, **kwargs) * self.code_to_base_value(quantity),
                        dtype=self._base_registry.get_dtype(quantity),
                    )

                return dec

            return _decorate_to_base
        elif conversion == "base_to_code":

            def _decorate_to_code(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return np.array(
                        function(*args, **kwargs) * self.base_to_code_value(quantity),
                        dtype=self._code_registry.get_dtype(quantity),
                    )

                return dec

            return _decorate_to_code
        elif conversion == "base_units":

            def _decorate_base_units(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return Q_(
                        np.array(
                            function(*args, **kwargs),
                            dtype=self._base_registry.get_dtype(quantity),
                        ),
                        self._base_registry[quantity],
                    )

                return dec

            return _decorate_base_units
        elif conversion == "code_units":

            def _decorate_code_units(function):
                @functools.wraps(function)
                def dec(*args, **kwargs):
                    return Q_(
                        np.array(
                            function(*args, **kwargs),
                            dtype=self._code_registry.get_dtype(quantity),
                        ),
                        self._code_registry[quantity],
                    )

                return dec

            return _decorate_code_units
        else:
            raise ValueError("Conversion type {} not implemented!".format(conversion))

    def code_to_base(self, quantity: str) -> callable:
        """
        Decorator for functions that returns a numpy array. Multiples the function output by the code to base units
        conversion factor

        Args:
            quantity (str):  Name of the quantity

        Returns:
            function: Decorated function

        """
        return self(quantity=quantity, conversion="code_to_base")

    def base_to_code(self, quantity: str) -> callable:
        """
        Decorator for functions that returns a numpy array. Multiples the function output by the base to code units
        conversion factor

        Args:
            quantity (str):  Name of the quantity

        Returns:
            function: Decorated function

        """
        return self(quantity=quantity, conversion="base_to_code")

    def code_units(self, quantity: str) -> callable:
        """
        Decorator for functions that returns a numpy array. Assigns the code unit of the quantity to the function output

        Args:
            quantity (str):  Name of the quantity

        Returns:
            function: Decorated function

        """
        return self(quantity=quantity, conversion="code_units")

    def base_units(self, quantity: str) -> callable:
        """
        Decorator for functions that returns a numpy array. Assigns the base unit of the quantity to the function output

        Args:
            quantity (str):  Name of the quantity

        Returns:
            function: Decorated function

        """
        return self(quantity=quantity, conversion="base_units")
