import unittest
import importlib
import pyiron_base


class ImportlibTest(unittest.TestCase):
    def test_multiple_imports(self):
        importlib.reload(pyiron_base)
        self.assertTrue(True)
