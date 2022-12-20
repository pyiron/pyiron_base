# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithFilledProject, ToyJob
from pyiron_base.jobs.job.path import JobPath


class TestLoaders(TestWithFilledProject):

    def test_load(self):
        with self.subTest("Basic functionality"):
            loaded = self.project.load("toy_1")
            self.assertIsInstance(
                loaded, ToyJob, msg="Expected to load the full object"
            )

            self.assertEqual(
                loaded.name,
                self.project.load.toy_1.name,
                # TODO: we'd rather compare the jobs directly, but equality comparison
                #       for jobs is not implemented
                msg="Attribute access should function the same as explicit calls"
            )

            self.assertEqual(
                len(self.project.job_table(recursive=False)),
                len(self.project.load.__dir__()),
                msg="Tab completion (`__dir__`) should see both jobs at this project "
                    "level"
            )

        with self.subTest("Without converting to object"):
            not_fully_loaded = self.project.load("toy_1", convert_to_object=False)
            self.assertIsInstance(
                not_fully_loaded,
                JobPath,
                msg="Expected to load only an HDF interface"
            )

            inspected = self.project.inspect("toy_1")
            self.assertIsInstance(
                inspected,
                JobPath,
                msg="Expected to load only an HDF interface"
            )