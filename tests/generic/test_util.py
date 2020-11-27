# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import warnings
from pyiron_base.generic.util import static_isinstance
from pyiron_base.generic.util import Deprecator, deprecate


class TestJobType(unittest.TestCase):
    def test_static_isinstance(self):
        self.assertTrue(
            static_isinstance(
                obj=list(), obj_type=["builtins.list", "__builtin__.list"]
            )
        )
        self.assertTrue(
            any(
                [
                    static_isinstance(obj=list(), obj_type="builtins.list"),
                    static_isinstance(obj=list(), obj_type="__builtin__.list"),
                ]
            )
        )
        self.assertRaises(TypeError, static_isinstance, list(), 1)

    def test_deprecate(self):
        """
        Function decorated with `deprecate` should raise a warning.
        """
        @deprecate
        def foo(a):
            return 2*a

        @deprecate("use baz instead", version="0.2.0")
        def bar(a):
            return 4*a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(foo(1), 2,
                             "Decorated function does not return original "
                             "return value")
        self.assertTrue(len(w) > 0, "No warning raised!")
        self.assertEqual(w[0].category, DeprecationWarning,
                        "Raised warning is not a DeprecationWarning")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(bar(1), 4,
                             "Decorated function does not return original "
                             "return value")

        expected_message = "use baz instead. It is not guaranteed to be in " \
                           "service in vers. 0.2.0"
        self.assertTrue( w[0].message.args[0].endswith(expected_message),
                        "Warning message does not reflect decorator arguments.")

if __name__ == "__main__":
    unittest.main()
