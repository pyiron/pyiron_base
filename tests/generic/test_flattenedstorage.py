import numpy as np

from pyiron_base._tests import TestWithProject
from pyiron_base.generic.flattenedstorage import FlattenedStorage

class TestFlattenedStorage(TestWithProject):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store = FlattenedStorage()

        cls.even = [ list(range(0, 2, 2)), list(range(2, 6, 2)), list(range(6, 12, 2)) ]
        cls.odd = np.array([ np.arange(1, 2, 2), np.arange(3, 6, 2), np.arange(7, 12, 2) ], dtype=object)


    def test_add_array(self):
        """Custom arrays added with add_array should be properly allocated with matching shape, dtype and fill"""

        store = FlattenedStorage()
        store.add_array("energy", per="chunk")
        store.add_array("forces", shape=(3,), per="element")
        store.add_array("fnorble", shape=(), dtype=np.int64, fill=0, per="element")

        self.assertTrue("energy" in store._per_chunk_arrays,
                        "no 'energy' array present after adding it with add_array()")
        self.assertEqual(store._per_chunk_arrays["energy"].shape, (store._num_chunks_alloc,),
                        "'energy' array has wrong shape")

        self.assertTrue("forces" in store._per_element_arrays,
                        "no 'forces' array present after adding it with add_array()")
        self.assertEqual(store._per_element_arrays["forces"].shape, (store._num_elements_alloc, 3),
                        "'forces' array has wrong shape")

        self.assertEqual(store._per_element_arrays["fnorble"].dtype, np.int64,
                         "'fnorble' array has wrong dtype after adding it with add_array()")
        self.assertTrue((store._per_element_arrays["fnorble"] == 0).all(),
                         "'fnorble' array not initialized with given fill value")

        try:
            store.add_array("energy", dtype=np.float64, per="chunk")
        except ValueError:
            self.fail("Duplicate calls to add_array should be ignored if types/shapes are compatible!")

        with self.assertRaises(ValueError, msg="Duplicate calls to add_array with invalid shape!"):
            store.add_array("energy", shape=(5,), dtype=np.float64, per="chunk")

        with self.assertRaises(ValueError, msg="Duplicate calls to add_array with invalid type!"):
            store.add_array("energy", dtype=np.complex64, per="chunk")

        try:
            store.add_array("forces", shape=(3,), dtype=np.float64, per="element")
        except ValueError:
            self.fail("Duplicate calls to add_array should be ignored if types/shapes are compatible!")

        with self.assertRaises(ValueError, msg="Duplicate calls to add_array with invalid type!"):
            store.add_array("forces", shape=(3,), dtype=np.complex64, per="element")

        with self.assertRaises(ValueError, msg="Duplicate calls to add_array with invalid shape!"):
            store.add_array("forces", shape=(5,), dtype=np.float64, per="element")

        with self.assertRaises(ValueError, msg="Cannot have per-chunk and per-element array of the same name!"):
            store.add_array("energy", per="element")

        with self.assertRaises(ValueError, msg="Cannot have per-chunk and per-element array of the same name!"):
            store.add_array("forces", per="chunk")

        with self.assertRaises(ValueError, msg="Invalid per value!"):
            store.add_array("foobar", per="xyzzy")

    def test_resize(self):
        """A dynamically resized container should behave exactly as a pre-allocated container."""

        foo = [ [1], [2, 3], [4, 5, 6] ]
        bar = [ 1, 2, 3 ]
        store_static = FlattenedStorage(num_chunks=3, num_elements=6)
        store_dynamic = FlattenedStorage(num_chunks=1, num_elements=1)

        store_static.add_array("foo", per="element")
        store_static.add_array("bar", per="chunk")

        for f, b in zip(foo, bar):
            store_static.add_chunk(len(f), foo=f, bar=b)
            store_dynamic.add_chunk(len(f), foo=f, bar=b)

        self.assertEqual(store_static.current_element_index, store_dynamic.current_element_index,
                         "Dynamic storeainer doesn't have the same current element after adding chunks.")
        self.assertEqual(store_static.current_chunk_index, store_dynamic.current_chunk_index,
                         "Dynamic storeainer doesn't have the same current chunk after adding chunks.")
        self.assertTrue( (store_static._per_element_arrays["foo"][:store_static.current_element_index] \
                            == store_dynamic._per_element_arrays["foo"][:store_dynamic.current_element_index]).all(),
                        "Array of per element quantity not equal after adding chunks.")
        self.assertTrue(np.isclose(store_static._per_chunk_arrays["bar"][:store_static.current_element_index],
                                   store_static._per_chunk_arrays["bar"][:store_dynamic.current_element_index]).all(),
                        "Array of per chunk quantity not equal after adding chunks.")

    def test_init(self):
        """Adding arrays via __init__ should be equivalent to adding them via add_chunks manually."""

        store = FlattenedStorage(even=self.even, odd=self.odd)
        self.assertEqual(len(store), 3, "Length of storage doesn't match length of initializer!")
        self.assertTrue( (store.get_array("even", 1) == np.array([2, 4])).all(),
                        "Values added via init don't match expected values!")
        self.assertTrue( (store.get_array("odd", 2) == np.array([7, 9, 11])).all(),
                        "Values added via init don't match expected values!")

        all_sum = [sum(e + o) for e, o in zip(self.even, self.odd)]
        try:
            FlattenedStorage(even=self.even, odd=self.odd, sum=all_sum)
        except ValueError:
            self.fail("Adding per chunk values to initializers raises error, but shouldn't!")

        with self.assertRaises(ValueError, msg="No error on inconsistent initializers!"):
            odd = self.odd.copy()
            odd[1] = [1,3,4]
            FlattenedStorage(even=self.even, odd=odd)

        with self.assertRaises(ValueError, msg="No error on initializers of different length!"):
            FlattenedStorage(foo=[ [1] ], bar=[ [2], [2, 3] ])

    def test_find_chunk(self):
        """find_chunk() should return the correct indices given an identifier."""

        store = FlattenedStorage()
        store.add_chunk(2, "first", integers=[1, 2])
        store.add_chunk(3, integers=[3, 4, 5])
        store.add_chunk(1, "third", integers=[5])

        self.assertEqual(store.find_chunk("first"), 0, "Incorrect chunk index returned!")
        self.assertEqual(store.find_chunk("1"), 1, "Incorrect chunk index returned for unamed identifier!")
        self.assertEqual(store.find_chunk("third"), 2, "Incorrect chunk index returned!")

        with self.assertRaises(KeyError, msg="No KeyError raised on non-existing identifier!"):
            store.find_chunk("asdf")

    def test_add_chunk_add_array(self):
        """Adding arrays via add_chunk and add_array should be equivalent."""

        cont = FlattenedStorage()
        cont.add_array("perchunk", shape=(3,), dtype=float, per="chunk")
        val = np.array([1,2,3])
        try:
            cont.add_chunk(1, perchunk=val[np.newaxis, :])
        except ValueError:
            # both checks below are regression tests for https://github.com/pyiron/pyiron_contrib/pull/197
            self.fail("add_chunk should not raise an exception when passed a value for an existing per-chunk array.")
        self.assertTrue(np.array_equal(val, cont.get_array("perchunk", 0)),
                        "add_chunk did not remove first axis on a per chunk array!")
        # test the same, but now let the array be created by add_chunk, instead of doing it on our own
        cont.add_chunk(2, perelem=[1,1], perchunk2=val[np.newaxis, :])
        self.assertTrue(np.array_equal(val, cont.get_array("perchunk2", 1)),
                        "add_chunk did not remove first axis on a per chunk array!")


    def test_get_array(self):
        """get_array should return the arrays for the correct structures."""

        store = FlattenedStorage()

        for n, e, o in zip( ("first", None, "third"), self.even, self.odd):
            store.add_chunk(len(e), identifier=n, even=e, odd=o, sum=sum(e + o))

        self.assertTrue(np.array_equal(store.get_array("even", 0), self.even[0]),
                        "get_array returns wrong array for numeric index!")

        self.assertTrue(np.array_equal(store.get_array("even", "first"), self.even[0]),
                        "get_array returns wrong array for string identifier!")

        self.assertTrue(np.array_equal(store.get_array("even", "1"), self.even[1]),
                        "get_array returns wrong array for automatic identifier!")

        self.assertTrue(np.array_equal(store.get_array("sum", 0), sum(self.even[0] + self.odd[0])),
                        "get_array returns wrong array for numeric index!")

        self.assertTrue(np.array_equal(store.get_array("sum", "first"), sum(self.even[0] + self.odd[0])),
                        "get_array returns wrong array for string identifier!")

        self.assertTrue(np.array_equal(store.get_array("sum", "1"), sum(self.even[1] + self.odd[1])),
                        "get_array returns wrong array for automatic identifier!")

        with self.assertRaises(KeyError, msg="Non-existing identifier!"):
            store.get_array("even", "foo")

    def test_get_array_full(self):
        """get_array should return full array for all chunks if not given frame."""

        store = FlattenedStorage(elem=[ [1], [2, 3], [4, 5, 6] ], chunk=[-1, -2, -3])
        elem = store.get_array("elem")
        self.assertTrue(np.array_equal(elem, [1, 2, 3, 4, 5, 6]),
                        f"get_array return did not return correct flat array, but {elem}.")
        chunk = store.get_array("chunk")
        self.assertTrue(np.array_equal(chunk, [-1, -2, -3]),
                        f"get_array return did not return correct flat array, but {chunk}.")

    def test_has_array(self):
        """hasarray should return correct information for added array; None otherwise."""

        store = FlattenedStorage()
        store.add_array("energy", per="chunk")
        store.add_array("forces", shape=(3,), per="element")

        info = store.has_array("energy")
        self.assertEqual(info["dtype"], np.float64, "has_array returns wrong dtype for per structure array.")
        self.assertEqual(info["shape"], (), "has_array returns wrong shape for per structure array.")
        self.assertEqual(info["per"], "chunk", "has_array returns wrong per for per structure array.")

        info = store.has_array("forces")
        self.assertEqual(info["dtype"], np.float64, "has_array returns wrong dtype for per atom array.")
        self.assertEqual(info["shape"], (3,), "has_array returns wrong shape for per atom array.")
        self.assertEqual(info["per"], "element", "has_array returns wrong per for per atom array.")

        self.assertEqual(store.has_array("missing"), None, "has_array does not return None for nonexisting array.")


    def test_hdf_chunklength_one(self):
        """Reading a storage with all chunks of length one should give back exactly what was written!"""
        # Regression test if all stored chunks are of length 1: there used to be a bug that read all arrays as per
        # element in this case
        store = FlattenedStorage()
        store.add_array('foo', dtype=np.int64, shape=(), per="element")
        store.add_array('bar', dtype=np.int64, shape=(), per="chunk")
        for i in range(5):
            store.add_chunk(1, foo=i, bar=i**2)
        hdf = self.project.create_hdf(self.project.path, "test")
        store.to_hdf(hdf)
        read = FlattenedStorage()
        try:
            read.from_hdf(hdf)
        except RuntimeError as e:
            self.fail(f"Reading storage from HDF failed with {e}")
        self.assertEqual(len(store), len(read), "Length not equal after reading from HDF!")
        for i in range(5):
            store_foo = store.get_array("foo", i)
            read_foo = read.get_array("foo", i)
            self.assertTrue(np.array_equal(store_foo, read_foo),
                            f"per element values not equal after reading from HDF! {store_foo} != {read_foo}")
            self.assertEqual(store.get_array("bar", i), read.get_array("bar", i),
                             "per chunk values not equal after reading from HDF!")

    def test_fill_value(self):
        """Test if fill values are correctly assigned when resizing an array and if self._fill_value is correctly read from hdf."""
        # Test for per chunk arrays
        store = FlattenedStorage()
        store.add_array("bar", per="chunk", dtype=bool, fill=True)
        store.add_array("foo", per="chunk")
        for i in range(3):
            store.add_chunk(1, bar=False, foo=i)
        store._resize_chunks(6)
        self.assertTrue(np.all(store._per_chunk_arrays["bar"][:3]==False), "value is overwritten when resizing")
        self.assertTrue(np.all(store._per_chunk_arrays["bar"][3:]==True), "fill value is not correctly set when resizing")
        self.assertTrue(np.all(store._per_chunk_arrays["foo"][0:3]==np.array((0,1,2))), "values in array changed on resizing")
        # Test for per element arrays
        store = FlattenedStorage()
        store.add_array("bar", per="element", fill=np.nan)
        store.add_array("foo", per="element")
        for i in range(1,4):
            store.add_chunk(i*2, bar=i*[i, i**2], foo=i*[i, i**2])
        store._resize_elements(15)
        self.assertTrue(np.all(store._per_element_arrays["foo"][:12]==store._per_element_arrays["bar"][:12]), "arrays are not equal up to resized part")
        self.assertTrue(np.all(np.isnan(store._per_element_arrays["bar"][12:])), "array should np.nan where not set")
        # Test hdf
        store = FlattenedStorage()
        store.add_array("bar", per="element", fill=np.nan)
        store.add_array("foo", per="element")
        store.add_array("fooTrue", per="chunk", dtype=bool, fill=True)
        store.add_array("barText", per="chunk", dtype="U4", fill="fill")
        hdf = self.project.create_hdf(self.project.path, "test_fill_values")
        store.to_hdf(hdf)
        read=FlattenedStorage()
        read.from_hdf(hdf)
        # normally it is possible to compare 2 dicts using ==, but np.nan!=np.nan so this has to be explicitly tested.
        for k, v in store._fill_values.items():
            if isinstance(v, float) and np.isnan(v):
                self.assertTrue(np.isnan(read._fill_values[k]))
            else:
                self.assertEqual(v, read._fill_values[k], "value read from hdf differs from original value")
        self.assertEqual(read._fill_values.keys(), store._fill_values.keys(), "keys read from hdf differ from original keys")
