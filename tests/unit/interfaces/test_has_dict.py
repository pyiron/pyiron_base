# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest

from pyiron_base.interfaces.has_dict import (
    HasDict,
    HasDictfromHDF,
    HasHDFfromDict,
    _from_dict_children,
    _to_dict_children,
    create_from_dict,
)
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base._tests import PyironTestCase


# ---------------------------------------------------------------------------
# Concrete test helpers
# ---------------------------------------------------------------------------

class SimpleHasDict(HasDict):
    """Minimal concrete HasDict implementation for testing."""

    def __init__(self, value=0):
        self.value = value

    def _from_dict(self, obj_dict, version=None):
        self.value = obj_dict.get("value", 0)

    def _to_dict(self):
        return {"value": self.value}


class HasDictCallsSuper(HasDict):
    """HasDict subclass that explicitly calls super() to cover abstract-body lines."""

    def __init__(self):
        self.value = 0

    def _from_dict(self, obj_dict, version=None):
        # Covers line 223 (the `pass` body of the abstract method)
        super()._from_dict(obj_dict, version)

    def _to_dict(self):
        # Covers line 247 (the `pass` body of the abstract method)
        result = super()._to_dict()
        return result if result is not None else {}


class SimplePureHasHDF(HasHDF):
    """A HasHDF that is NOT a HasDict - used to test line 147 in _to_dict_children."""

    def __init__(self, val=42):
        self.val = val

    def _from_hdf(self, hdf, version=None):
        self.val = hdf.get("val", 42)

    def _to_hdf(self, hdf):
        hdf["val"] = self.val

    def _get_hdf_group_name(self):
        return None

    def _to_dict(self):
        # Required so HasDictfromHDF.to_dict(self) works (line 147 path)
        from pyiron_base.storage.hdfio import DummyHDFio
        hdf = DummyHDFio(None, "/")
        self.to_hdf(hdf)
        return hdf.to_dict()


class SimpleHasDictFromHDF(HasDictfromHDF):
    """Minimal HasDictfromHDF with no group name."""

    def __init__(self, value=0):
        self.value = value

    def _from_hdf(self, hdf, version=None):
        self.value = hdf.get("value", 0)

    def _to_hdf(self, hdf):
        hdf["value"] = self.value

    def _get_hdf_group_name(self):
        return None


class GroupedHasDictFromHDF(HasDictfromHDF):
    """HasDictfromHDF that uses a group name."""

    def __init__(self, value=0):
        self.value = value

    def _from_hdf(self, hdf, version=None):
        self.value = hdf.get("value", 0)

    def _to_hdf(self, hdf):
        hdf["value"] = self.value

    def _get_hdf_group_name(self):
        return "mygroup"


class SimpleHasHDFfromDict(HasHDFfromDict):
    """Minimal HasHDFfromDict for testing the HDF delegation."""

    def __init__(self, value=0):
        self.value = value

    def _from_dict(self, obj_dict, version=None):
        self.value = obj_dict.get("value", 0)

    def _to_dict(self):
        return {"value": self.value}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCreateFromDict(PyironTestCase):
    """Tests for the create_from_dict function."""

    def test_raises_without_type_key(self):
        """Line 52: ValueError when TYPE key missing."""
        with self.assertRaises(ValueError):
            create_from_dict({"value": 1})

    def test_creates_object_from_valid_dict(self):
        """Round-trip: create_from_dict restores an object serialized with to_dict."""
        original = SimpleHasDict(value=7)
        d = original.to_dict()
        restored = create_from_dict(d)
        self.assertIsInstance(restored, SimpleHasDict)
        self.assertEqual(restored.value, 7)


class TestFromDictChildren(PyironTestCase):
    """Tests for _from_dict_children."""

    def test_non_dict_values_returned_unchanged(self):
        result = _from_dict_children({"a": 1, "b": "hello"})
        self.assertEqual(result, {"a": 1, "b": "hello"})

    def test_nested_dict_without_type_info_recursed(self):
        result = _from_dict_children({"nested": {"x": 10}})
        self.assertEqual(result["nested"]["x"], 10)

    def test_dict_with_all_type_keys_creates_object(self):
        """Line 117: return create_from_dict(inner_dict) when all keys present."""
        original = SimpleHasDict(value=3)
        d = original.to_dict()
        # d must have NAME, TYPE, OBJECT, DICT_VERSION
        result = _from_dict_children({"child": d})
        self.assertIsInstance(result["child"], SimpleHasDict)
        self.assertEqual(result["child"].value, 3)


class TestToDictChildren(PyironTestCase):
    """Tests for _to_dict_children."""

    def test_plain_values_kept(self):
        result = _to_dict_children({"x": 1, "y": "str"})
        self.assertEqual(result["x"], 1)
        self.assertEqual(result["y"], "str")

    def test_has_dict_value_serialized(self):
        child = SimpleHasDict(value=5)
        result = _to_dict_children({"child": child})
        self.assertIn("child/value", result)

    def test_has_hdf_value_serialized_via_has_dict_from_hdf(self):
        """Line 147: HasHDF (not HasDict) value converted via HasDictfromHDF.to_dict."""
        child = SimplePureHasHDF(val=99)
        result = _to_dict_children({"child": child})
        self.assertIn("child/val", result)
        self.assertEqual(result["child/val"], 99)


class TestHasDictAbstractBodies(PyironTestCase):
    """Covers the `pass` bodies of abstract methods in HasDict (lines 223, 247)."""

    def test_abstract_from_dict_body_covered(self):
        """Line 223: super()._from_dict() call reaches the abstract pass body."""
        obj = HasDictCallsSuper()
        obj._from_dict({"value": 5})  # should not raise

    def test_abstract_to_dict_body_covered(self):
        """Line 247: super()._to_dict() call reaches the abstract pass body."""
        obj = HasDictCallsSuper()
        result = obj._to_dict()
        self.assertEqual(result, {})


class TestHasHDFfromDict(PyironTestCase):
    """Tests for HasHDFfromDict (lines 282, 285)."""

    def test_from_hdf_roundtrip(self):
        """Line 282: _from_hdf delegates to from_dict."""
        from pyiron_base.storage.hdfio import DummyHDFio
        obj = SimpleHasHDFfromDict(value=11)
        hdf = DummyHDFio(None, "/")
        obj.to_hdf(hdf)

        obj2 = SimpleHasHDFfromDict()
        obj2.from_hdf(hdf)
        self.assertEqual(obj2.value, 11)

    def test_to_hdf_writes_dict(self):
        """Line 285: _to_hdf delegates to to_dict."""
        from pyiron_base.storage.hdfio import DummyHDFio
        obj = SimpleHasHDFfromDict(value=22)
        hdf = DummyHDFio(None, "/")
        obj.to_hdf(hdf)
        d = hdf.to_dict()
        self.assertIn("value", d)
        self.assertEqual(d["value"], 22)


class TestHasDictfromHDF(PyironTestCase):
    """Tests for HasDictfromHDF (lines 299-321)."""

    def test_instantiate(self):
        """Lines 299-300: instantiate creates an object from obj_dict."""
        original = SimpleHasDictFromHDF(value=7)
        d = original.to_dict()
        restored = SimpleHasDictFromHDF.instantiate(d)
        self.assertIsInstance(restored, SimpleHasDictFromHDF)

    def test_from_dict_without_group_name(self):
        """Line 310: _from_dict without group_name uses flat DummyHDFio."""
        original = SimpleHasDictFromHDF(value=13)
        d = original.to_dict()
        restored = SimpleHasDictFromHDF()
        restored.from_dict(d)
        self.assertEqual(restored.value, 13)

    def test_from_dict_with_group_name(self):
        """Lines 307-308: _from_dict with group_name wraps data in a sub-group."""
        original = GroupedHasDictFromHDF(value=42)
        d = original.to_dict()
        restored = GroupedHasDictFromHDF()
        restored.from_dict(d)
        self.assertEqual(restored.value, 42)

    def test_to_dict_without_group_name(self):
        """Line 321: _to_dict without group_name returns flat data."""
        obj = SimpleHasDictFromHDF(value=9)
        d = obj.to_dict()
        self.assertIn("value", d)
        self.assertEqual(d["value"], 9)

    def test_to_dict_with_group_name(self):
        """Line 319: _to_dict with group_name returns data from the group."""
        obj = GroupedHasDictFromHDF(value=55)
        d = obj.to_dict()
        self.assertIn("value", d)
        self.assertEqual(d["value"], 55)

    def test_full_roundtrip_via_create_from_dict(self):
        """End-to-end round-trip through create_from_dict."""
        original = SimpleHasDictFromHDF(value=77)
        d = original.to_dict()
        restored = create_from_dict(d)
        self.assertIsInstance(restored, SimpleHasDictFromHDF)
        self.assertEqual(restored.value, 77)


if __name__ == "__main__":
    unittest.main()
