import numpy as np

from pyiron_base._tests import TestWithProject
from pyiron_base.storage.flattenedstorage import FlattenedStorage


class TestFlattenedStorage(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store = FlattenedStorage()

        cls.even = [list(range(0, 2, 2)), list(range(2, 6, 2)), list(range(6, 12, 2))]
        cls.odd = np.array(
            [np.arange(1, 2, 2), np.arange(3, 6, 2), np.arange(7, 12, 2)], dtype=object
        )
        cls.even_sum = list(map(sum, cls.even))
        cls.odd_sum = list(map(sum, cls.odd))

    def test_add_array(self):
        """Custom arrays added with add_array should be properly allocated with matching shape, dtype and fill"""

        store = FlattenedStorage()
        store.add_array("energy", per="chunk")
        store.add_array("forces", shape=(3,), per="element")
        store.add_array("fnorble", shape=(), dtype=np.int64, fill=0, per="element")

        self.assertTrue(
            "energy" in store._per_chunk_arrays,
            "no 'energy' array present after adding it with add_array()",
        )
        self.assertEqual(
            store._per_chunk_arrays["energy"].shape,
            (store._num_chunks_alloc,),
            "'energy' array has wrong shape",
        )

        self.assertTrue(
            "forces" in store._per_element_arrays,
            "no 'forces' array present after adding it with add_array()",
        )
        self.assertEqual(
            store._per_element_arrays["forces"].shape,
            (store._num_elements_alloc, 3),
            "'forces' array has wrong shape",
        )

        self.assertEqual(
            store._per_element_arrays["fnorble"].dtype,
            np.int64,
            "'fnorble' array has wrong dtype after adding it with add_array()",
        )
        self.assertTrue(
            (store._per_element_arrays["fnorble"] == 0).all(),
            "'fnorble' array not initialized with given fill value",
        )

        try:
            store.add_array("energy", dtype=np.float64, per="chunk")
        except ValueError:
            self.fail(
                "Duplicate calls to add_array should be ignored if types/shapes are compatible!"
            )

        with self.assertRaises(
            ValueError, msg="Duplicate calls to add_array with invalid shape!"
        ):
            store.add_array("energy", shape=(5,), dtype=np.float64, per="chunk")

        with self.assertRaises(
            ValueError, msg="Duplicate calls to add_array with invalid type!"
        ):
            store.add_array("energy", dtype=np.complex64, per="chunk")

        try:
            store.add_array("forces", shape=(3,), dtype=np.float64, per="element")
        except ValueError:
            self.fail(
                "Duplicate calls to add_array should be ignored if types/shapes are compatible!"
            )

        with self.assertRaises(
            ValueError, msg="Duplicate calls to add_array with invalid type!"
        ):
            store.add_array("forces", shape=(3,), dtype=np.complex64, per="element")

        with self.assertRaises(
            ValueError, msg="Duplicate calls to add_array with invalid shape!"
        ):
            store.add_array("forces", shape=(5,), dtype=np.float64, per="element")

        with self.assertRaises(
            ValueError,
            msg="Cannot have per-chunk and per-element array of the same name!",
        ):
            store.add_array("energy", per="element")

        with self.assertRaises(
            ValueError,
            msg="Cannot have per-chunk and per-element array of the same name!",
        ):
            store.add_array("forces", per="chunk")

        with self.assertRaises(ValueError, msg="Invalid per value!"):
            store.add_array("foobar", per="xyzzy")

    def test_resize(self):
        """A dynamically resized container should behave exactly as a pre-allocated container."""

        foo = [[1], [2, 3], [4, 5, 6]]
        bar = [1, 2, 3]
        store_static = FlattenedStorage(num_chunks=3, num_elements=6)
        store_dynamic = FlattenedStorage(num_chunks=1, num_elements=1)

        store_static.add_array("foo", per="element")
        store_static.add_array("bar", per="chunk")

        for f, b in zip(foo, bar):
            store_static.add_chunk(len(f), foo=f, bar=b)
            store_dynamic.add_chunk(len(f), foo=f, bar=b)

        self.assertEqual(
            store_static.current_element_index,
            store_dynamic.current_element_index,
            "Dynamic storeainer doesn't have the same current element after adding chunks.",
        )
        self.assertEqual(
            store_static.current_chunk_index,
            store_dynamic.current_chunk_index,
            "Dynamic storeainer doesn't have the same current chunk after adding chunks.",
        )
        self.assertTrue(
            (
                store_static._per_element_arrays["foo"][
                    : store_static.current_element_index
                ]
                == store_dynamic._per_element_arrays["foo"][
                    : store_dynamic.current_element_index
                ]
            ).all(),
            "Array of per element quantity not equal after adding chunks.",
        )
        self.assertTrue(
            np.isclose(
                store_static._per_chunk_arrays["bar"][
                    : store_static.current_element_index
                ],
                store_static._per_chunk_arrays["bar"][
                    : store_dynamic.current_element_index
                ],
            ).all(),
            "Array of per chunk quantity not equal after adding chunks.",
        )

        # Regression test, where adding chunks to a storage with no pre-allocation raised an error
        store_empty = FlattenedStorage(num_chunks=0, num_elements=0)
        store_empty.add_array("foo", per="element")
        store_empty.add_array("bar", per="chunk")

        try:
            for f, b in zip(foo, bar):
                store_empty.add_chunk(len(f), foo=f, bar=b)
        except:
            self.fail("Empty storage should be correctly resized when adding chunks!")

    def test_init(self):
        """Adding arrays via __init__ should be equivalent to adding them via add_chunks manually."""

        store = FlattenedStorage(even=self.even, odd=self.odd)
        self.assertEqual(
            len(store), 3, "Length of storage doesn't match length of initializer!"
        )
        self.assertTrue(
            (store.get_array("even", 1) == np.array([2, 4])).all(),
            "Values added via init don't match expected values!",
        )
        self.assertTrue(
            (store.get_array("odd", 2) == np.array([7, 9, 11])).all(),
            "Values added via init don't match expected values!",
        )

        all_sum = [sum(e + o) for e, o in zip(self.even, self.odd)]
        try:
            FlattenedStorage(even=self.even, odd=self.odd, sum=all_sum)
        except ValueError:
            self.fail(
                "Adding per chunk values to initializers raises error, but shouldn't!"
            )

        with self.assertRaises(
            ValueError, msg="No error on inconsistent initializers!"
        ):
            odd = self.odd.copy()
            odd[1] = [1, 3, 4]
            FlattenedStorage(even=self.even, odd=odd)

        with self.assertRaises(
            ValueError, msg="No error on initializers of different length!"
        ):
            FlattenedStorage(foo=[[1]], bar=[[2], [2, 3]])

    def test_find_chunk(self):
        """find_chunk() should return the correct indices given an identifier."""

        store = FlattenedStorage()
        store.add_chunk(2, "first", integers=[1, 2])
        store.add_chunk(3, integers=[3, 4, 5])
        store.add_chunk(1, "third", integers=[5])

        self.assertEqual(
            store.find_chunk("first"), 0, "Incorrect chunk index returned!"
        )
        self.assertEqual(
            store.find_chunk("1"),
            1,
            "Incorrect chunk index returned for unamed identifier!",
        )
        self.assertEqual(
            store.find_chunk("third"), 2, "Incorrect chunk index returned!"
        )

        with self.assertRaises(
            KeyError, msg="No KeyError raised on non-existing identifier!"
        ):
            store.find_chunk("asdf")

    def test_add_chunk_add_array(self):
        """Adding arrays via add_chunk and add_array should be equivalent."""

        cont = FlattenedStorage()
        cont.add_array("perchunk", shape=(3,), dtype=float, per="chunk")
        val = np.array([1, 2, 3])
        try:
            cont.add_chunk(1, perchunk=val[np.newaxis, :])
        except ValueError:
            # both checks below are regression tests for https://github.com/pyiron/pyiron_contrib/pull/197
            self.fail(
                "add_chunk should not raise an exception when passed a value for an existing per-chunk array."
            )
        self.assertEqual(
            val,
            cont.get_array("perchunk", 0),
            "add_chunk did not remove first axis on a per chunk array!",
        )
        # test the same, but now let the array be created by add_chunk, instead of doing it on our own
        cont.add_chunk(2, perelem=[1, 1], perchunk2=val[np.newaxis, :])
        self.assertEqual(
            val,
            cont.get_array("perchunk2", 1),
            "add_chunk did not remove first axis on a per chunk array!",
        )

    def test_get_array(self):
        """get_array should return the arrays for the correct structures."""

        store = FlattenedStorage()

        for n, e, o in zip(("first", None, "third"), self.even, self.odd):
            store.add_chunk(len(e), identifier=n, even=e, odd=o, sum=sum(e + o))

        self.assertEqual(
            store.get_array("even", 0),
            np.array(self.even[0]),
            "get_array returns wrong array for numeric index!",
        )

        self.assertEqual(
            store.get_array("even", "first"),
            np.array(self.even[0]),
            "get_array returns wrong array for string identifier!",
        )

        self.assertEqual(
            store.get_array("even", "1"),
            np.array(self.even[1]),
            "get_array returns wrong array for automatic identifier!",
        )

        self.assertEqual(
            store.get_array("sum", 0),
            sum(self.even[0] + self.odd[0]),
            "get_array returns wrong array for numeric index!",
        )

        self.assertEqual(
            store.get_array("sum", "first"),
            sum(self.even[0] + self.odd[0]),
            "get_array returns wrong array for string identifier!",
        )

        self.assertEqual(
            store.get_array("sum", "1"),
            sum(self.even[1] + self.odd[1]),
            "get_array returns wrong array for automatic identifier!",
        )

        with self.assertRaises(KeyError, msg="Non-existing identifier!"):
            store.get_array("even", "foo")

    def test_get_array_full(self):
        """get_array should return full array for all chunks if not given frame."""

        store = FlattenedStorage(elem=[[1], [2, 3], [4, 5, 6]], chunk=[-1, -2, -3])
        elem = store.get_array("elem")
        self.assertEqual(
            elem,
            np.array([1, 2, 3, 4, 5, 6]),
            f"get_array return did not return correct flat array, but {elem}.",
        )
        chunk = store.get_array("chunk")
        self.assertEqual(
            chunk,
            np.array([-1, -2, -3]),
            f"get_array return did not return correct flat array, but {chunk}.",
        )

    def test_get_array_filled(self):
        """get_array_filled should return a padded array of all elements in the storage."""

        store = FlattenedStorage(elem=[[1], [2, 3], [4, 5, 6]], chunk=[-1, -2, -3])
        store.add_array("fill", fill=23.42)
        store.set_array("fill", 0, [-1])
        store.set_array("fill", 1, [-2, -3])
        store.set_array("fill", 2, [-4, -5, -6])
        store.add_array("complex", shape=(3,), dtype=np.float64)
        store.set_array("complex", 0, [[1, 1, 1]])
        store.set_array(
            "complex",
            1,
            [
                [2, 2, 2],
                [2, 2, 2],
            ],
        )
        store.set_array(
            "complex",
            2,
            [
                [3, 3, 3],
                [3, 3, 3],
                [3, 3, 3],
            ],
        )
        val = store.get_array_filled("elem")
        self.assertEqual(val.shape, (3, 3), "shape not correct!")
        self.assertEqual(
            val,
            np.array([[1, -1, -1], [2, 3, -1], [4, 5, 6]]),
            "values in returned array not the same as in original array!",
        )
        self.assertEqual(
            store.get_array_filled("fill")[0, 1], 23.42, "incorrect fill value!"
        )
        val = store.get_array_filled("complex")
        self.assertEqual(val.shape, (3, 3, 3), "shape not correct!")
        self.assertEqual(
            store.get_array("chunk"),
            store.get_array_filled("chunk"),
            "get_array_filled does not give same result as get_array for per chunk array",
        )

    def test_get_array_ragged(self):
        """get_array_ragged should return a raggend array of all elements in the storage."""

        store = FlattenedStorage(elem=[[1], [2, 3], [4, 5, 6]], chunk=[-1, -2, -3])
        val = store.get_array_ragged("elem")
        self.assertEqual(val.shape, (3,), "shape not correct!")
        for i, v in enumerate(val):
            self.assertEqual(
                len(v),
                store._per_chunk_arrays["length"][i],
                f"array {i} has incorrect length!",
            )
            self.assertEqual(
                v,
                np.array([[1], [2, 3], [4, 5, 6]][i]),
                f"array {i} has incorrect values, {v}!",
            )
        (
            self.assertEqual(
                store.get_array("chunk"),
                store.get_array_ragged("chunk"),
            ),
            "get_array_ragged does not give same result as get_array for per chunk array",
        )

    def test_get_array_ragged_dtype_stability(self):
        """get_array_ragged should (only!) convert top-most dimension to dtype=object and be of shape (n,)"""
        # regression test
        store = FlattenedStorage(elem=[[1, 2], [3, 4], [5, 6]])
        ragged = store.get_array_ragged("elem")
        self.assertEqual(ragged.dtype, np.dtype("O"), "Top most dtype not object!")
        self.assertEqual(len(ragged.shape), 1, "Shape not (n,)!")
        for array in store._per_element_arrays:
            for a in ragged:
                self.assertEqual(
                    a.dtype,
                    store._per_element_arrays[array].dtype,
                    "Nested array returned from get_array_ragged has wrong dtype!",
                )

    def test_has_array(self):
        """has_array should return correct information for added array; None otherwise."""

        store = FlattenedStorage()
        store.add_array("energy", per="chunk")
        store.add_array("forces", shape=(3,), per="element")

        info = store.has_array("energy")
        self.assertEqual(
            info["dtype"],
            np.float64,
            "has_array returns wrong dtype for per structure array.",
        )
        self.assertEqual(
            info["shape"], (), "has_array returns wrong shape for per structure array."
        )
        self.assertEqual(
            info["per"], "chunk", "has_array returns wrong per for per structure array."
        )

        info = store.has_array("forces")
        self.assertEqual(
            info["dtype"],
            np.float64,
            "has_array returns wrong dtype for per atom array.",
        )
        self.assertEqual(
            info["shape"], (3,), "has_array returns wrong shape for per atom array."
        )
        self.assertEqual(
            info["per"], "element", "has_array returns wrong per for per atom array."
        )

        self.assertEqual(
            store.has_array("missing"),
            None,
            "has_array does not return None for nonexisting array.",
        )

    def test_list_arrays(self):
        """list_arrays should return the string names of all arrays."""
        store = FlattenedStorage()
        self.assertEqual(
            sorted(store.list_arrays()),
            sorted(["identifier", "length", "start_index"]),
            "Array names of empty storage don't match default arrays!",
        )
        self.assertEqual(
            store.list_arrays(only_user=True),
            ["identifier"],
            "User array names of empty storage contains more than `identifier`!",
        )
        store.add_array("energy", per="chunk")
        store.add_array("forces", shape=(3,), per="element")
        self.assertEqual(
            sorted(store.list_arrays()),
            sorted(["identifier", "length", "start_index", "energy", "forces"]),
            "Array names don't match added ones!",
        )
        self.assertEqual(
            sorted(store.list_arrays(only_user=True)),
            sorted(["identifier", "energy", "forces"]),
            "Array names don't match added ones!",
        )

    def test_hdf_empty(self):
        """Writing an empty storage should result in an empty storage when reading."""
        store = FlattenedStorage()
        hdf = self.project.create_hdf(self.project.path, "empty")
        store.to_hdf(hdf, "empty")
        store_read = hdf["empty"].to_object()
        self.assertEqual(
            len(store),
            len(store_read),
            "Length of empty storage not equal after writing/reading!",
        )

        store = FlattenedStorage(num_chunks=5, num_elements=10)
        hdf = self.project.create_hdf(self.project.path, "empty")
        store.to_hdf(hdf, "empty")
        store_read = hdf["empty"].to_object()
        self.assertEqual(
            len(store),
            len(store_read),
            "Length of empty storage not equal after writing/reading!",
        )

    def test_sample(self):
        """Calling sample should return a storage with the selected chunks only."""
        store = FlattenedStorage(
            even=self.even, odd=self.odd, even_sum=self.even_sum, odd_sum=self.odd_sum
        )
        all_sub = store.sample(lambda s, i: True)
        self.assertEqual(
            len(store), len(all_sub), "Length not equal after sampling all chunks!"
        )
        empty_sub = store.sample(lambda s, i: False)
        self.assertEqual(len(empty_sub), 0, "Length not zero after sampling no chunks!")
        some_sub = store.sample(lambda s, i: i % 2 == 1)
        self.assertEqual(len(some_sub), 1, "Length not one after sampling one chunk!")
        self.assertEqual(
            "1", some_sub.get_array("identifier", 0), "sample selected wrong chunk!"
        )
        for k, v in store._per_chunk_arrays.items():
            self.assertTrue(
                k in some_sub._per_chunk_arrays,
                f"Chunk array {k} not present in sample storage!",
            )
            self.assertEqual(
                v.shape[1:],
                some_sub._per_chunk_arrays[k].shape[1:],
                f"Chunk array {k} present in sample storage, but wrong shape!",
            )
            self.assertEqual(
                v.dtype,
                some_sub._per_chunk_arrays[k].dtype,
                f"Chunk array {k} present in sample storage, but wrong dtype!",
            )

        for k, v in store._per_element_arrays.items():
            self.assertTrue(
                k in some_sub._per_element_arrays,
                f"Element array {k} not present in sample storage!",
            )
            self.assertEqual(
                v.shape[1:],
                some_sub._per_element_arrays[k].shape[1:],
                f"Element array {k} present in sample storage, but wrong shape!",
            )
            self.assertEqual(
                v.dtype,
                some_sub._per_element_arrays[k].dtype,
                f"Element array {k} present in sample storage, but wrong dtype!",
            )

    def test_join(self):
        """All arrays should be present in joined storage."""
        even_store = FlattenedStorage(even=self.even, even_sum=self.even_sum)
        odd_store = FlattenedStorage(odd=self.odd, odd_sum=self.odd_sum)
        both_store = even_store.copy().join(odd_store)
        self.assertTrue(
            (both_store["even"] == even_store["even"]).all(),
            "Per element array 'even' not present after join!",
        )
        self.assertTrue(
            (both_store["odd"] == odd_store["odd"]).all(),
            "Per chunk array 'odd' not present after join!",
        )
        self.assertTrue(
            (both_store["even_sum"] == even_store["even_sum"]).all(),
            "Per element array 'even_sum' not present after join!",
        )
        self.assertTrue(
            (both_store["odd_sum"] == odd_store["odd_sum"]).all(),
            "Per chunk array 'odd_sum' not present after join!",
        )

    def test_join_conflict(self):
        """Joining storages with same named arrays should raise an error or rename the arrays."""
        even_store = FlattenedStorage(even=self.even, even_sum=self.even_sum)
        even2_store = FlattenedStorage(even=self.even, even_sum=self.even_sum)
        with self.assertRaises(
            ValueError,
            msg="Joining should raise an error if storages share an array name",
        ):
            even_store.join(even2_store)

        for lsuffix, rsuffix in (("_left", ""), ("", "_suffix"), ("_left", "_right")):
            with self.subTest(lsuffix=lsuffix, rsuffix=rsuffix):
                join_store = even_store.copy().join(
                    even2_store, lsuffix=lsuffix, rsuffix=rsuffix
                )
                self.assertTrue(
                    join_store.has_array(f"even{lsuffix}"),
                    "left array not present after join.",
                )
                self.assertTrue(
                    join_store.has_array(f"even{rsuffix}"),
                    "right array not present after join.",
                )
                self.assertTrue(
                    join_store.has_array(f"even_sum{lsuffix}"),
                    "left array not present after join.",
                )
                self.assertTrue(
                    join_store.has_array(f"even_sum{rsuffix}"),
                    "right array not present after join.",
                )
                self.assertTrue(
                    np.array_equal(join_store[f"even{lsuffix}"], even_store["even"]),
                    "right array not the same after join.",
                )
                self.assertTrue(
                    np.array_equal(join_store[f"even{rsuffix}"], even2_store["even"]),
                    "left array not the same after join.",
                )

    def test_split(self):
        """split should deep copy all the selected arrays to the new storage."""
        store = FlattenedStorage(
            even=self.even, odd=self.odd, even_sum=self.even_sum, odd_sum=self.odd_sum
        )
        odd_store = store.split(("odd", "odd_sum"))

        self.assertTrue(
            "odd" in odd_store._per_element_arrays,
            "Per element array 'odd' not present after split!",
        )
        self.assertTrue(
            (store["odd"] == odd_store["odd"]).all(),
            "Per element array 'odd' incorrectly copied after split!",
        )
        self.assertTrue(
            "odd_sum" in odd_store._per_chunk_arrays,
            "Per chunk array 'odd_sum' not present after split!",
        )
        self.assertTrue(
            (store["odd_sum"] == odd_store["odd_sum"]).all(),
            "Per chunk array 'odd_sum' incorrectly copied after split!",
        )

        odd_before = odd_store["odd"]
        odd_sum_before = odd_store["odd_sum"]
        store["odd", 2] *= 2
        store["odd_sum", 2] *= 2
        self.assertTrue(
            (odd_before == odd_store["odd"]).all(),
            "Per element array changed in copy when original is!",
        )
        self.assertTrue(
            (odd_sum_before == odd_store["odd_sum"]).all(),
            "Per chunk array changed in copy when original is!",
        )

    def test_getitem_setitem(self):
        """Using __getitem__/__setitem__ should be equivalent to using get_array/set_array."""
        store = FlattenedStorage(even=self.even, odd=self.odd, mylen=[1, 2, 3])
        for i in range(len(store)):
            self.assertEqual(
                store["even", i],
                store.get_array("even", i),
                f"getitem returned different value ({store['even', i]}) than get_array ({store.get_array('even', i)}) for chunk {i}",
            )
            self.assertEqual(
                store["mylen", i],
                store.get_array("mylen", i),
                f"getitem returned different value ({store['mylen', i]}) than get_array ({store.get_array('mylen', i)}) for chunk {i}",
            )
        self.assertEqual(
            store["even"],
            store.get_array("even"),
            f"getitem returned different value ({store['even']}) than get_array ({store.get_array('even')})",
        )
        self.assertEqual(
            store["mylen"],
            store.get_array("mylen"),
            f"getitem returned different value ({store['mylen']}) than get_array ({store.get_array('mylen')})",
        )
        store["even", 0] = [4]
        store["even", 1] = [2, 0]
        store["mylen", 0] = 4
        self.assertEqual(
            store.get_array("mylen", 0), 4, "setitem did not set item correctly."
        )
        self.assertEqual(
            store.get_array("even", 0), [4], "setitem did not set item correctly."
        )
        self.assertEqual(
            store.get_array("even", 1),
            np.array([2, 0]),
            "setitem did not set item correctly.",
        )

        with self.assertRaises(
            IndexError, msg="Calling setitem with out index doesn't raise Error!"
        ):
            store["mylen"] = [1, 2, 3]

    def test_hdf_chunklength_one(self):
        """Reading a storage with all chunks of length one should give back exactly what was written!"""
        # Regression test if all stored chunks are of length 1: there used to be a bug that read all arrays as per
        # element in this case
        store = FlattenedStorage()
        store.add_array("foo", dtype=np.int64, shape=(), per="element")
        store.add_array("bar", dtype=np.int64, shape=(), per="chunk")
        for i in range(5):
            store.add_chunk(1, foo=i, bar=i**2)
        hdf = self.project.create_hdf(self.project.path, "test")
        store.to_hdf(hdf)
        read = FlattenedStorage()
        try:
            read.from_hdf(hdf)
        except RuntimeError as e:
            self.fail(f"Reading storage from HDF failed with {e}")
        self.assertEqual(
            len(store), len(read), "Length not equal after reading from HDF!"
        )
        for i in range(5):
            store_foo = store.get_array("foo", i)
            read_foo = read.get_array("foo", i)
            self.assertEqual(
                store_foo,
                read_foo,
                f"per element values not equal after reading from HDF! {store_foo} != {read_foo}",
            )
            self.assertEqual(
                store.get_array("bar", i),
                read.get_array("bar", i),
                "per chunk values not equal after reading from HDF!",
            )

    def test_fill_value(self):
        """Test if fill values are correctly assigned when resizing an array and if self._fill_value is correctly read from hdf."""
        # Test for per chunk arrays
        store = FlattenedStorage()
        store.add_array("bar", per="chunk", dtype=bool, fill=True)
        store.add_array("foo", per="chunk")
        for i in range(3):
            store.add_chunk(1, bar=False, foo=i)
        store._resize_chunks(6)
        self.assertTrue(
            np.all(store._per_chunk_arrays["bar"][:3] == False),
            "value is overwritten when resizing",
        )
        self.assertTrue(
            np.all(store._per_chunk_arrays["bar"][3:] == True),
            "fill value is not correctly set when resizing",
        )
        self.assertTrue(
            np.all(store._per_chunk_arrays["foo"][0:3] == np.array((0, 1, 2))),
            "values in array changed on resizing",
        )
        # Test for per element arrays
        store = FlattenedStorage()
        store.add_array("bar", per="element", fill=np.nan)
        store.add_array("foo", per="element")
        for i in range(1, 4):
            store.add_chunk(i * 2, bar=i * [i, i**2], foo=i * [i, i**2])
        store._resize_elements(15)
        self.assertTrue(
            np.all(
                store._per_element_arrays["foo"][:12]
                == store._per_element_arrays["bar"][:12]
            ),
            "arrays are not equal up to resized part",
        )
        self.assertTrue(
            np.all(np.isnan(store._per_element_arrays["bar"][12:])),
            "array should np.nan where not set",
        )
        # Test hdf
        store = FlattenedStorage()
        store.add_array("bar", per="element", fill=np.nan)
        store.add_array("foo", per="element")
        store.add_array("fooTrue", per="chunk", dtype=bool, fill=True)
        store.add_array("barText", per="chunk", dtype="U4", fill="fill")
        hdf = self.project.create_hdf(self.project.path, "test_fill_values")
        store.to_hdf(hdf)
        read = FlattenedStorage()
        read.from_hdf(hdf)
        # normally it is possible to compare 2 dicts using ==, but np.nan!=np.nan so this has to be explicitly tested.
        for k, v in store._fill_values.items():
            if isinstance(v, float) and np.isnan(v):
                self.assertTrue(np.isnan(read._fill_values[k]))
            else:
                self.assertEqual(
                    v,
                    read._fill_values[k],
                    "value read from hdf differs from original value",
                )
        self.assertEqual(
            read._fill_values.keys(),
            store._fill_values.keys(),
            "keys read from hdf differ from original keys",
        )

    def test_copy(self):
        """copy should give the same data and be a deep copy."""
        store = FlattenedStorage(
            even=self.even, odd=self.odd, even_sum=self.even_sum, odd_sum=self.odd_sum
        )
        copy = store.copy()

        for k in "even", "odd", "even_sum", "odd_sum":
            with self.subTest(k=k):
                self.assertTrue(
                    (store[k] == copy[k]).all(), f"Array {k} not equal after copy!"
                )

        even_before = copy["even"]
        even_sum_before = copy["even_sum"]
        store["even", 2] *= 2
        store["even_sum", 2] *= 2
        self.assertTrue(
            (even_before == copy["even"]).all(),
            "Per element array changed in copy when original is!",
        )
        self.assertTrue(
            (even_sum_before == copy["even_sum"]).all(),
            "Per chunk array changed in copy when original is!",
        )

    def test_string_resize(self):
        """string arrays should be automatically resized, when longer strings are added"""

        store = FlattenedStorage()
        store.add_array("chunkstr", dtype="<3U", per="chunk")
        store.add_array("elemstr", shape=(2,), dtype="<3U", per="element")
        for i in range(1, 14):
            # default length for identifiers is 20 chars, so we need to push it a bit more
            store.add_chunk(
                1, identifier="i" * i * 3, chunkstr="a" * i, elemstr=["a" * i] * 2
            )
        for i in range(1, 14):
            self.assertEqual(
                store["chunkstr", i - 1],
                "a" * i,
                "Per chunk string array not correctly resized!",
            )
            self.assertEqual(
                store["elemstr", i - 1].tolist(),
                [["a" * i] * 2],
                "Per element string array not correctly resized!",
            )
            self.assertEqual(
                store["identifier", i - 1],
                "i" * i * 3,
                "Chunk identifiers not correctly resized!",
            )

    def test_extend(self):
        store = FlattenedStorage()
        store.add_array("foo", fill=np.nan, per="chunk")
        store.add_array("bar", shape=(2,), fill=0, per="element")
        foo = []
        bar = []
        store2 = FlattenedStorage()
        store2.add_array("foo", fill=np.nan, per="chunk")
        store2.add_array("bar", shape=(2,), fill=0, per="element")
        store2.add_array("foobar", fill=0, dtype=int, per="chunk")
        store2.add_array("barfoo", shape=(2,), fill=0.0, per="element")

        for i in range(0, 3):
            # default length for identifiers is 20 chars, so we need to push it a bit more
            foo_val = i
            bar_val = np.array([i, i**2] * i).reshape(i, 2)
            foo.append(foo_val)
            bar.append(bar_val)
            store.add_chunk(i, identifier=f"ID{i}", foo=foo_val, bar=bar_val)

        for i in range(3, 5):
            # default length for identifiers is 20 chars, so we need to push it a bit more
            foo_val = i
            bar_val = np.array([i, i**2] * i).reshape(i, 2)
            foo.append(foo_val)
            bar.append(bar_val)
            store2.add_chunk(
                i,
                identifier=f"ID{i}",
                foo=foo_val,
                bar=bar_val,
                foobar=foo_val * 2,
                barfoo=bar_val * 3,
            )

        foo = np.array(foo)
        bar = np.concatenate(bar)
        foobar = foo * 2
        barfoo = bar * 3
        store.extend(store2)
        self.assertTrue(np.all(foo == store.get_array("foo")))
        self.assertTrue(np.all(bar == store.get_array("bar")))
        self.assertTrue(np.all(foobar[3:5] == store.get_array("foobar")[3:5]))
        self.assertTrue(np.all(barfoo[3:10] == store.get_array("barfoo")[3:10]))

    def test_del_array(self):
        store = FlattenedStorage(
            elem1=[[1], [2, 3], [4, 5, 6]],
            elem2=[[1], [2, 3], [4, 5, 6]],
            chunk1=[-1, -2, -3],
            chunk2=[-1, -2, -3],
        )
        with self.subTest("ignore_missing"):
            with self.assertRaises(
                KeyError, msg="del_array doesn't raise an error on missing key"
            ):
                store.del_array("foobar")
            with self.assertRaises(
                KeyError, msg="__delitem__ doesn't raise an error on missing key"
            ):
                del store["foobar"]
            try:
                store.del_array("foobar", ignore_missing=True)
            except KeyError:
                self.fail("del_array raises error with ignore_missing present")

        with self.subTest("per chunk"):
            del store["chunk1"]
            self.assertTrue(
                "chunk1" not in store.list_arrays(),
                "Per chunk array still present after __delitem__",
            )
            store.del_array("chunk2")
            self.assertTrue(
                "chunk2" not in store.list_arrays(),
                "Per chunk array still present after del_array",
            )

        with self.subTest("per element"):
            del store["elem1"]
            self.assertTrue(
                "elem1" not in store.list_arrays(),
                "Per element array still present after __delitem__",
            )
            store.del_array("elem2")
            self.assertTrue(
                "elem2" not in store.list_arrays(),
                "Per element array still present after del_array",
            )

    def test_to_pandas(self):
        """to_pandas should return a dataframe with user defined arrays."""

        store = FlattenedStorage(
            even=self.even,
            odd=self.odd,
            even_sum=self.even_sum,
            odd_sum=self.odd_sum,
        )

        arrays = store.list_arrays(only_user=True)
        dfc = store.to_pandas()
        self.assertEqual(
            sorted(arrays), sorted(dfc.columns), "Not all columns present in dataframe!"
        )
        for a in arrays:
            with self.subTest(array=a):
                for i, (elem_df, elem_st) in enumerate(
                    zip(dfc[a], store.get_array_ragged(a))
                ):
                    self.assertEqual(
                        elem_df,
                        elem_st,
                        f"Element {i} in dataframe not equal to original: {elem_df}!={elem_st}!",
                    )

        dfe = store.to_pandas(explode=True)
        for a in arrays:
            with self.subTest(array=a):
                if a == "identifier":
                    self.assertEqual(
                        dfe[a].to_numpy().dtype,
                        np.dtype("O"),
                        "dtype not conserved with explode=True!",
                    )
                else:
                    self.assertEqual(
                        dfe[a].to_numpy().dtype,
                        store[a].dtype,
                        "dtype not conserved with explode=True!",
                    )
