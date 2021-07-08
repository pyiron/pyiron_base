from pyiron_base._tests import TestWithProject
from pyiron_base.generic.datacontainer import DataContainer
from pyiron_base.generic.hdfstub import HDFStub

import numpy as np

class TestHDFStub(TestWithProject):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.hdf = cls.project.create_hdf(cls.project.path, "hdf")
        cls.hdf["number"] = 42
        cls.hdf["array"] = np.arange(100)
        cls.data = DataContainer([1, 2, "three", 4.0])
        cls.data.to_hdf(cls.hdf, "data")

    def test_load(self):
        """Lazily and eagerly read values should be the same."""
        self.assertEqual(HDFStub(self.hdf, "number").load(), self.hdf["number"],
                         "Simple number read with load() not equal to eager read.")
        self.assertTrue(np.all( HDFStub(self.hdf, "array").load() == self.hdf["array"] ),
                        "Numpy array read with load() not equal to eager read.")
        for v1, v2 in zip(HDFStub(self.hdf, "data").load(), self.data):
            self.assertEqual(v1, v2, "Data container values read with load() not equal to original container.")
