# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import warnings
from pyiron_base.generic.util import static_isinstance
from pyiron_base.generic.util import deprecate, deprecate_soon
from pyiron_base.generic.util import ImportAlarm


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


class TestDeprecator(PyironTestCase):
    def test_deprecate(self):
        """Function decorated with `deprecate` should raise a warning."""
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

        @deprecate_soon
        def baz(a):
            return 3*a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(baz(1), 3,
                             "Decorated function does not return original "
                             "return value")
        self.assertEqual(w[0].category, PendingDeprecationWarning,
                        "Raised warning is not a PendingDeprecationWarning")

    def test_deprecate_args(self):
        """DeprecationWarning should only be raised when the given arguments occur."""
        @deprecate(arguments={"bar": "use foo instead"})
        def foo(a, foo=None, bar=None):
            return 2*a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(foo(1, bar=True), 2,
                             "Decorated function does not return original "
                             "return value")
        self.assertTrue(len(w) > 0, "No warning raised!")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(foo(1, foo=True), 2,
                             "Decorated function does not return original "
                             "return value")
        self.assertEqual(len(w), 0, "Warning raised, but deprecated argument was not given.")

    def test_deprecate_kwargs(self):
        """DeprecationWarning should only be raised when the given arguments occur, also when given via kwargs."""
        @deprecate(bar="use baz instead")
        def foo(a, bar=None, baz=None):
            return 2*a

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(foo(1, bar=True), 2,
                             "Decorated function does not return original "
                             "return value")
        self.assertTrue(len(w) > 0, "No warning raised!")

        with warnings.catch_warnings(record=True) as w:
            self.assertEqual(foo(1, baz=True), 2,
                             "Decorated function does not return original "
                             "return value")
        self.assertEqual(len(w), 0, "Warning raised, but deprecated argument was not given.")

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

class TestImportAlarm(PyironTestCase):

    def setUp(self):
        self.import_alarm = ImportAlarm()

        @self.import_alarm
        def add_one(x):
            return x + 1

        with ImportAlarm("Broken import") as alarm_broken:
            import ASDF

        @alarm_broken
        def add_two(x):
            return x + 2

        with ImportAlarm("Working import") as alarm_working:
            import sys

        @alarm_working
        def add_three(x):
            return x + 3

        self.add_one = add_one
        self.add_two = add_two
        self.add_three = add_three

    def test_no_warning(self):
        with warnings.catch_warnings(record=True) as w:
            self.add_one(0)
        self.assertEqual(len(w), 0, "Expected no warnings, but got {} warnings.".format(len(w)))

    def test_has_warning(self):
        self.import_alarm.message = "Now add_one should throw an ImportWarning"

        with warnings.catch_warnings(record=True) as w:
            self.add_one(1)
        self.assertEqual(len(w), 1, "Expected one warning, but got {} warnings.".format(len(w)))

    def test_context(self):
        """
        Usage via context manager should give same results and not suppress other errors.
        """

        with warnings.catch_warnings(record=True) as w:
            self.add_two(0)
        self.assertEqual(len(w), 1, "Expected one warning, but got {} warnings.".format(len(w)))

        with warnings.catch_warnings(record=True) as w:
            self.add_three(0)
        self.assertEqual(len(w), 0, "Expected one warning, but got {} warnings.".format(len(w)))

        with self.assertRaises(ZeroDivisionError, msg="Context manager should swallow unrelated exceptions"), \
             ImportAlarm("Unrelated"):
            print(1/0)

if __name__ == "__main__":
    unittest.main()
