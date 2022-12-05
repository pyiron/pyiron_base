import unittest

from pyiron_base.utils.error import retry

class TestRetry(unittest.TestCase):

    def test_return_value(self):
        """retry should return the exact value that the function returns."""
        def func():
            return 42

        self.assertEqual(func(), retry(func, error=ValueError, msg=""),
                         "retry returned a different value!")

    def test_unrelated_exception(self):
        """retry should not catch exception that are not explicitely passed."""
        def func():
            raise ValueError()
        with self.assertRaises(
                ValueError,
                msg="retry caught an exception it was not supposed to!"
        ):
            retry(func, error=TypeError, msg="")
    def test_exception(self):
        """retry should catch explicitely passed exceptions."""
        class Func():
            """Small helper to simulate a stateful function."""
            def __init__(self):
                self.n = 0
            def __call__(self):
                self.n += 1
                if self.n < 4:
                    raise ValueError(self.n)
                else:
                    return self.n
        func = Func()
        try:
            retry(func, error=ValueError, msg="", delay=1e-6)
        except ValueError:
            self.fail("retry did not catch exception!")

        func = Func()
        with self.assertRaises(
                ValueError,
                msg="retry did re-raise exception after insufficient tries!"
        ):
            retry(func, error=ValueError, msg="", at_most=2, delay=1e-6)
