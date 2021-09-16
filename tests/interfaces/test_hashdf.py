import pyiron_base.interfaces.has_hdf
from pyiron_base._tests import PyironTestCase

class TestHasHDF(PyironTestCase):

    @property
    def docstring_module(self):
        return pyiron_base.interfaces.has_hdf
