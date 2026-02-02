import unittest

import numpy as np
from pyiron_base._tests import TestWithFilledProject
from pyiron_base import GenericJob


try:
    import git

    git_not_available = False
except ImportError:
    git_not_available = True


def _test_array(start=0):
    return np.arange(start, start + 100, dtype=object).reshape(5, 20)


class TestMaintenance(TestWithFilledProject):
    def setUp(self) -> None:
        super().setUp()
        job: GenericJob = self.project["toy_1"]
        job["user/some"] = _test_array(5)
        job["user/some"] = _test_array()
        self.initial_toy_1_hdf_file_size = job.project_hdf5.file_size()

    def _assert_setup(self):
        job_hdf = self.project["toy_1"].project_hdf5
        array = self.project["toy_1/user/some"]
        self.assertEqual(array, _test_array())
        self.assertAlmostEqual(job_hdf.file_size(), self.initial_toy_1_hdf_file_size)

    def _assert_hdf_rewrite(self):
        job_hdf = self.project["toy_1"].project_hdf5
        array = self.project["toy_1/user/some"]
        self.assertEqual(array, _test_array())
        self.assertLess(job_hdf.file_size(), self.initial_toy_1_hdf_file_size)

    @unittest.skipIf(
        git_not_available,
        "gitpython is not available so the gitpython related tests are skipped.",
    )
    def test_repository_status(self):
        df = self.project.maintenance.get_repository_status()
        self.assertIn(
            "pyiron_base",
            df.Module.values,
            "Environment dependent, but pyiron_base should be in there!",
        )

    def test_local_defragment_storage(self):
        self._assert_setup()
        self.project.maintenance.local.defragment_storage()
        self._assert_hdf_rewrite()

    def test_update_base_to_current(self):
        self._assert_setup()

        with self.subTest("Version bigger 0"):
            with self.assertRaises(ValueError):
                self.project.maintenance.update.base_to_current("1.0.2")
                self._assert_setup()

        with self.subTest(msg="Version not smaller 4, no action expected!"):
            self.project.maintenance.update.base_to_current("0.4.3")
            self._assert_setup()

        with self.subTest(msg="Version < 0.4: should run"):
            self.project.maintenance.update.base_to_current("0.3.999")
            self._assert_hdf_rewrite()

    def test_update_v03_to_v04_None(self):
        self._assert_setup()
        self.project.maintenance.update.base_v0_3_to_v0_4()
        self._assert_hdf_rewrite()

    def test_update_v03_to_v04_self(self):
        self._assert_setup()
        self.project.maintenance.update.base_v0_3_to_v0_4(project=self.project)
        self._assert_hdf_rewrite()

    def test_update_v03_to_v04_str(self):
        self._assert_setup()
        self.project.maintenance.update.base_v0_3_to_v0_4(project=self.project.path)
        self._assert_hdf_rewrite()
