# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithFilledProject, ToyJob
from pyiron_base.jobs.job.path import JobPath


class TestLoaders(TestWithFilledProject):

    # def test_load(self):
    #     loaded = self.project.load("toy_1")
    #     self.assertIsInstance(
    #         loaded, ToyJob, msg="Expected to load the full object"
    #     )
    #
    #     self.assertEqual(
    #         loaded.name,
    #         self.project.load.toy_1.name,
    #         # TODO: we'd rather compare the jobs directly, but equality comparison
    #         #       for jobs is not implemented
    #         msg="Attribute access should function the same as explicit calls"
    #     )
    #
    #     self.assertEqual(
    #         len(self.project.job_table(recursive=False)),
    #         len(self.project.load.__dir__()),
    #         msg="Tab completion (`__dir__`) should see both jobs at this project "
    #             "level"
    #     )

    # def test_inspect(self):
    #     not_fully_loaded = self.project.load("toy_1", convert_to_object=False)
    #     self.assertIsInstance(
    #         not_fully_loaded,
    #         JobPath,
    #         msg="Expected to load only an HDF interface"
    #     )
    #
    #     inspected = self.project.inspect("toy_1")
    #     self.assertIsInstance(
    #         inspected,
    #         JobPath,
    #         msg="Expected to load only an HDF interface"
    #     )

    def test_without_database(self):
        settings_configuration = self.project.state.settings.configuration.copy()
        import logging
        cols = ["id", "job"]
        logging.warning("With database: " + str(self.project.job_table().loc[:, cols]))
        self.project.state.update({'disable_database': True})
        logging.warning("Without database: " + str(self.project.job_table().loc[:, cols]))
        logging.warning("At path" + str(self.project.path))
        logging.warning("Loader job table: " + str(self.project.load._job_table))
        logging.warning("Loader job name: " + str(self.project.load._job_names))
        logging.warning("Map filter: " + str(self._job_names == "toy_1"))
        # logging.warning("Index for toy_1: " + str(self.project.load._id_from_name("toy_1")))
        with self.subTest("Works without the database"):
            self.assertIsInstance(
                self.project.load.toy_1, ToyJob, msg="Expected to load the full object"
            )
            self.assertIsInstance(
                self.project.inspect.toy_2, JobPath, msg="Expected just the HDF object"
            )

        self.project.state.update({'disable_database': False})
        with self.subTest("Doesn't cause any trouble turning the database back on"):
            self.assertIsInstance(
                self.project.load.toy_1, ToyJob, msg="Expected to load the full object"
            )
            self.assertIsInstance(
                self.project.inspect.toy_2, JobPath, msg="Expected just the HDF object"
            )

        self.project.state.update(settings_configuration)
