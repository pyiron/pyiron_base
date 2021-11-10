from pyiron_base._tests import TestWithProject
import numpy as np
import timeit

class TestFileHDFio(TestWithProject):
    def test_ragged(self):
        """Storing normal arrays should be faster than storing ragged arrays."""
        hdf = self.project.create_hdf(self.project.path, "ragged")
        regular = np.random.rand(10, 100, 100)
        ragged  = [np.random.rand(100 + i, 100) for i in range(-5, 5)]

        time_regular = timeit.timeit("hdf['array'] = array", number=50, globals={'hdf': hdf, 'array': regular})
        time_ragged = timeit.timeit("hdf['array'] = array", number=50, globals={'hdf': hdf, 'array': ragged})
        self.assertGreater(time_ragged, time_regular,
                           "Storing an regular array is not faster than a ragged one!")
