# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import warnings
from pyiron_base.utils.instance import static_isinstance
<<<<<<< snippets_deprecate
=======
from pyiron_base.utils.deprecate import deprecate, deprecate_soon
>>>>>>> main
from pyiron_base._tests import PyironTestCase


class TestJobType(PyironTestCase):
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


<<<<<<< snippets_deprecate
=======
class TestDeprecator(PyironTestCase):
    def test_deprecate(self):
        """Function decorated with `deprecate` should raise a warning."""

        @deprecate
        def foo(a):
            return 2 * a

        @deprecate("use baz instead", version="0.2.0")
        def bar(a):
            return 4 * a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                foo(1), 2, "Decorated function does not return original " "return value"
            )
        self.assertTrue(len(w) > 0, "No warning raised!")
        self.assertEqual(
            w[0].category,
            DeprecationWarning,
            "Raised warning is not a DeprecationWarning",
        )

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                bar(1), 4, "Decorated function does not return original " "return value"
            )

        expected_message = (
            "use baz instead. It is not guaranteed to be in " "service in vers. 0.2.0"
        )
        self.assertTrue(
            w[0].message.args[0].endswith(expected_message),
            "Warning message does not reflect decorator arguments.",
        )

        @deprecate_soon
        def baz(a):
            return 3 * a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                baz(1), 3, "Decorated function does not return original " "return value"
            )
        self.assertEqual(
            w[0].category,
            PendingDeprecationWarning,
            "Raised warning is not a PendingDeprecationWarning",
        )

    def test_deprecate_args(self):
        """DeprecationWarning should only be raised when the given arguments occur."""

        @deprecate(arguments={"bar": "use foo instead"})
        def foo(a, foo=None, bar=None):
            return 2 * a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                foo(1, bar=True),
                2,
                "Decorated function does not return original " "return value",
            )
        self.assertTrue(len(w) > 0, "No warning raised!")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                foo(1, foo=True),
                2,
                "Decorated function does not return original " "return value",
            )
        self.assertEqual(
            len(w), 0, "Warning raised, but deprecated argument was not given."
        )

    def test_deprecate_kwargs(self):
        """DeprecationWarning should only be raised when the given arguments occur, also when given via kwargs."""

        @deprecate(bar="use baz instead")
        def foo(a, bar=None, baz=None):
            return 2 * a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                foo(1, bar=True),
                2,
                "Decorated function does not return original " "return value",
            )
        self.assertTrue(len(w) > 0, "No warning raised!")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(
                foo(1, baz=True),
                2,
                "Decorated function does not return original " "return value",
            )
        self.assertEqual(
            len(w), 0, "Warning raised, but deprecated argument was not given."
        )

    def test_instances(self):
        """Subsequent calls to a Deprecator instance must not interfere with each other."""

        @deprecate(bar="use baz instead")
        def foo(bar=None, baz=None):
            pass

        @deprecate(baz="use bar instead")
        def food(bar=None, baz=None):
            pass

        with warnings.catch_warnings(record=True) as w:
            foo(bar=True)
            food(baz=True)
        self.assertEqual(len(w), 2, "Not all warnings preserved.")


>>>>>>> main
if __name__ == "__main__":
    unittest.main()
