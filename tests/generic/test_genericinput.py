# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from pyiron_base.generic.genericinput import GenericInput, InputField
import unittest


class Example(GenericInput):
    '''Input parameters for MD calculations.'''
    temperature = InputField(
        name="temperature", data_type=float, doc="Run at this temperature"
    )
    steps = InputField(
        name="steps", data_type=int, doc="How many steps to integrate"
    )
    timestep = InputField(
        name="timestep", data_type=float, default=1e-15, doc="Time step for the integration in fs"
    )


class TestGenericInput(unittest.TestCase):
    def test_string(self):
        ex = Example()
        doc = ex.__doc__.split('\n')
        self.assertEqual(doc[-3], "\ttemperature (<class 'float'>): Run at this temperature")
        self.assertEqual(doc[-2], "\tsteps (<class 'int'>): How many steps to integrate")
        self.assertEqual(doc[-1], "\ttimestep (<class 'float'>): Time step for the integration in fs")


if __name__ == "__main__":
    unittest.main()
