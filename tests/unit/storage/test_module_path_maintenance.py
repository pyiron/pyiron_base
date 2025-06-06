import pyiron_base.maintenance.generic

from pyiron_base import DataContainer
from pyiron_base._tests import TestWithProject, ToyJob


class TestModulePath(TestWithProject):
    def test_add_module_conversion(self):
        """module paths should only be able to be added once!"""

        # need to add real modules here, because they need to be importable.
        pyiron_base.maintenance.generic.add_module_conversion("foo.bar", "os.path")
        with self.assertRaises(
            ValueError, msg="Adding paths twice should raise an error!"
        ):
            pyiron_base.maintenance.generic.add_module_conversion("foo.bar", "os")

    def test_maintenance(self):
        """Objects should be loaded correctly after maintenance is run."""

        # dummy data
        dc = DataContainer({"a": 42, "b": [1, 2, 3]})
        self.project.data["test_data"] = dc
        self.project.data.write()

        hdf = self.project.create_hdf(self.project.path, "project_data")["../data"]
        # manipulate type to fake an old module
        old_path_project_data = "project.data.module"
        hdf["test_data__index_0/TYPE"] = (
            f"<class '{old_path_project_data}.DataContainer'>"
        )

        job = self.project.create_job(ToyJob, "test_job")
        job.run()

        # manipulate type to fake an old module
        old_path_job = "job.module"
        job.project_hdf5["TYPE"] = f"<class '{old_path_job}.ToyJob'>"

        pyiron_base.maintainance.maintenance.add_module_conversion(
            old_path_project_data, DataContainer.__module__
        )
        pyiron_base.maintainance.maintenance.add_module_conversion(
            old_path_job, ToyJob.__module__
        )

        with self.assertRaises(
            RuntimeError,
            msg="Project data should raise a special exception for objects that can be fixed.",
        ):
            self.project.data.read()
            self.project.data[
                "test_data"
            ]  # need to access it, otherwise lazy loading hides the error

        with self.assertRaises(
            RuntimeError,
            msg="Job loading should raise a special exception for objects that can be fixed.",
        ):
            self.project["test_job"]

        self.project.maintenance.local.update_hdf_types()

        try:
            self.project.data.read()
            self.project.data[
                "test_data"
            ]  # need to access it, otherwise lazy loading hides the error
            self.project["test_job"]
        except:
            self.fail("Objects still not loadable after maintenance!")

        self.assertFalse(
            old_path_project_data in hdf["test_data__index_0/TYPE"],
            "Module path not updated in project data!",
        )

        self.assertFalse(
            old_path_job in job.project_hdf5["TYPE"],
            "Module path not updated in job hdf5!",
        )
