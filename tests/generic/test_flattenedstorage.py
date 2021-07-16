import unittest
import numpy as np

from pyiron_base.generic.flattenedstorage import FlattenedStorage

class TestFlattenedStorage(unittest.TestCase):

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
