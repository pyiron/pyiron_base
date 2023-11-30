import pyiron_base.storage.hdfio

from pyiron_base import DataContainer
from pyiron_base._tests import TestWithProject

class TestModulePath(TestWithProject):

    def test_add_module_conversion(self):
        """module paths should only be able to be added once!"""

        # need to add real modules here, because they need to be importable.
        pyiron_base.storage.hdfio.add_module_conversion_path('foo.bar', 'os.path')
        with self.assertRaises(ValueError, msg="Adding paths twice should raise an error!"):
            pyiron_base.storage.hdfio.add_module_conversion_path('foo.bar', 'os')

    def test_sys_patch(self):
        """Objects should be loaded correctly after a module conversion path is added."""

        # dummy data
        dc = DataContainer({"a": 42, "b": [1,2,3]})
        hdf = self.project.create_hdf(self.project.path, "test")
        dc.to_hdf(hdf, group_name="test_data")

        # manipulate type to fake an old module
        type_str = hdf["test_data/TYPE"]
        old_path = "my.old.module"
        hdf["test_data/TYPE"] = f"<class '{old_path}.DataContainer'>"
        pyiron_base.storage.hdfio.add_module_conversion_path(old_path, DataContainer.__module__)

        try:
            hdf["test_data"].to_object()
        except:
            self.fail("Could not load object after conversion path was added!")

        self.assertEqual(hdf["test_data/TYPE"], type_str,
                         "TYPE not updated after object loaded from remapped module!")
