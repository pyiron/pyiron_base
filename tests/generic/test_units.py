# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import numpy as np
from pyiron_base._tests import TestWithDocstrings
import pyiron_base
from pyiron_base.generic.units import PyironUnitRegistry, UnitConverter
import pint

pint_registry = pint.UnitRegistry()


class TestUnits(TestWithDocstrings):

    @property
    def docstring_module(self):
        return pyiron_base.generic.units

    def test_units(self):
        base_units = PyironUnitRegistry()
        base_units.add_quantity(quantity="energy", unit=pint_registry.eV)
        self.assertRaises(ValueError, base_units.add_quantity, quantity="energy", unit="eV")
        dim_less_q = pint_registry.Quantity
        base_units.add_quantity(quantity="dimensionless_integer_quantity", unit=dim_less_q(1), data_type=int)
        self.assertIsInstance(base_units.quantity_dict, dict)
        self.assertIsInstance(base_units.unit_dict, dict)
        self.assertEqual(base_units.unit_dict["energy"], pint_registry.eV)
        self.assertIsInstance(base_units.dtype_dict, dict)
        self.assertIsInstance(base_units.dtype_dict["energy"], float.__class__)
        code_units = PyironUnitRegistry()
        # Define unit kJ/mol
        code_units.add_quantity(quantity="energy",
                                unit=pint_registry.kilocal / (pint_registry.mol * pint_registry.N_A))
        code_units.add_labels(labels=["energy_tot", "energy_pot"], quantity="energy")
        # Raise Error for undefined quantity
        self.assertRaises(KeyError, code_units.add_labels, labels=["mean_forces"], quantity="force")
        self.assertTrue(code_units["energy"], code_units["energy_tot"])
        self.assertTrue(code_units["energy"], code_units["energy_pot"])
        # Define converter
        unit_converter = UnitConverter(base_registry=base_units, code_registry=code_units)
        self.assertAlmostEqual(round(unit_converter.code_to_base_value("energy"), 3), 0.043)
        # Raise error if quantity not defined in any of the unit registries
        self.assertRaises(KeyError, unit_converter.code_to_base_value, "dimensionless_integer_quantity")
        self.assertRaises(KeyError, code_units.get_dtype, "dimensionless_integer_quantity")
        # Define dimensionless quantity in the code units registry
        code_units.add_quantity(quantity="dimensionless_integer_quantity", unit=dim_less_q(1), data_type=int)
        self.assertIsInstance(code_units.get_dtype("dimensionless_integer_quantity"), int.__class__)
        self.assertIsInstance(code_units.get_dtype("energy_tot"), float.__class__)
        self.assertAlmostEqual(unit_converter.code_to_base_value("dimensionless_integer_quantity"), 1)
        self.assertAlmostEqual(unit_converter.code_to_base_value("energy")
                               * unit_converter.base_to_code_value("energy"), 1)

        # Use decorator to convert units
        @unit_converter.code_to_base(quantity="energy")
        def return_ones_base():
            return np.ones(10)

        @unit_converter.base_to_code(quantity="energy")
        def return_ones_code():
            return np.ones(10)

        @unit_converter.base_units(quantity="energy")
        def return_ones_ev():
            return np.ones(10)

        @unit_converter.code_units(quantity="energy")
        def return_ones_kj_mol():
            return np.ones(10)

        self.assertEqual(1 * return_ones_kj_mol().units, 1 * code_units["energy"])
        self.assertEqual(1 * return_ones_ev().units, 1 * base_units["energy"])
        self.assertTrue(np.allclose(return_ones_base(), np.ones(10) * 0.0433641))
        self.assertTrue(np.allclose(return_ones_base() * return_ones_code(), np.ones(10) * 1))
        self.assertRaises(ValueError, unit_converter, quantity="energy", conversion="gibberish")
        # Define dimensionally incorrect units
        code_units.add_quantity(quantity="energy",
                                unit=pint_registry.N * pint_registry.metre ** 2)
        # Check if dimensionality error raised
        self.assertRaises(pint.DimensionalityError, UnitConverter, base_registry=base_units, code_registry=code_units)
        # Try SI units
        code_units.add_quantity(quantity="energy",
                                unit=pint_registry.N * pint_registry.metre)
        unit_converter = UnitConverter(base_registry=base_units, code_registry=code_units)
        self.assertAlmostEqual(round(unit_converter.code_to_base_value("energy") / 1e18, 3), 6.242)
        # Raise error if quantity not defined in base class
        code_units.add_quantity(quantity="force", unit=pint_registry.N)
        self.assertRaises(ValueError, UnitConverter, base_registry=base_units, code_registry=code_units)
