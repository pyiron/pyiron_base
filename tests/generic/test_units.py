# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.generic.units import UnitsBase, UnitConverter
import unittest
import pint

pint_registry = pint.UnitRegistry()


class TestUnits(unittest.TestCase):

    def test_units(self):
        base_units = UnitsBase()
        base_units.add_quantity(quantity="energy", unit=pint_registry.eV)
        code_units = UnitsBase()
        # Define unit kJ/mol
        code_units.add_quantity(quantity="energy",
                                unit=1e3 * pint_registry.cal / (pint_registry.mol * pint_registry.N_A))
        code_units.add_labels(labels=["energy_tot", "energy_pot"], quantity="energy")
        self.assertTrue(code_units["energy"], code_units["energy_tot"])
        self.assertTrue(code_units["energy"], code_units["energy_pot"])
        # Define converter
        converter = UnitConverter(base_units=base_units, code_units=code_units)
        self.assertAlmostEqual(round(converter.code_to_base_value("energy"), 3), 0.043)
        self.assertAlmostEqual(converter.code_to_base_value("energy") * converter.base_to_code_value("energy"), 1e3)
        # Define dimensionally incorrect units
        code_units.add_quantity(quantity="energy",
                                unit=pint_registry.N * pint_registry.metre ** 2)
        converter = UnitConverter(base_units=base_units, code_units=code_units)
        # Check if dimensionality error raised
        self.assertRaises(pint.DimensionalityError, converter.code_to_base_value, "energy")
        # Try wacky units
        code_units.add_quantity(quantity="energy",
                                unit=pint_registry.N * pint_registry.metre)
        converter = UnitConverter(base_units=base_units, code_units=code_units)
        self.assertAlmostEqual(round(converter.code_to_base_value("energy") / 1e18, 3), 6.242)
