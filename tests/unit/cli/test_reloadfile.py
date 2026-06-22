# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

from pyiron_base.cli import reloadfile


class TestReloadfileCli(unittest.TestCase):
    def test_register(self):
        parser = ArgumentParser()
        reloadfile.register(parser)
        args = parser.parse_args(
            ["-i", "input/toy_job.h5", "-o", "output/toy_job.h5"]
        )
        self.assertEqual(args.input_path, os.path.abspath("input/toy_job.h5"))
        self.assertEqual(args.output_path, os.path.abspath("output/toy_job.h5"))

    def _setup_mocks(self, open_hdf_mock, shutil_mock, state_mock, project_mock):
        hdf = MagicMock()
        hdf.keys.return_value = ["toy_job"]
        open_hdf_mock.return_value.__enter__.return_value = hdf
        open_hdf_mock.return_value.__exit__.return_value = False

        job_reload = MagicMock()
        project_instance = MagicMock()
        project_instance.load_from_jobpath.return_value = job_reload
        project_mock.return_value = project_instance

        return job_reload, project_instance

    @patch("pyiron_base.cli.reloadfile.Project")
    @patch("pyiron_base.cli.reloadfile.state")
    @patch("pyiron_base.cli.reloadfile.shutil")
    @patch("pyiron_base.cli.reloadfile._open_hdf")
    def test_main_without_database(
        self, open_hdf_mock, shutil_mock, state_mock, project_mock
    ):
        state_mock.database.top_path.return_value = None
        job_reload, project_instance = self._setup_mocks(
            open_hdf_mock, shutil_mock, state_mock, project_mock
        )

        parser = ArgumentParser()
        reloadfile.register(parser)
        args = parser.parse_args(["-i", "input/toy_job.h5", "-o", "output/toy_job.h5"])
        reloadfile.main(args)

        expected_project_path = os.path.join(os.path.abspath("."), "toy_job.h5")
        shutil_mock.copy.assert_any_call(args.input_path, expected_project_path)
        shutil_mock.copy.assert_any_call(expected_project_path, args.output_path)

        _, kwargs = project_instance.load_from_jobpath.call_args
        self.assertIsNone(kwargs["job_id"])
        self.assertTrue(kwargs["convert_to_object"])
        self.assertEqual(kwargs["db_entry"]["job"], "toy_job")
        self.assertEqual(kwargs["db_entry"]["subjob"], "/toy_job")
        self.assertEqual(
            kwargs["db_entry"]["project"],
            os.path.dirname(expected_project_path) + "/",
        )

        self.assertTrue(job_reload.status.initialized)
        self.assertTrue(job_reload.server.run_mode.modal)
        job_reload.run.assert_called_once()

    @patch("pyiron_base.cli.reloadfile.Project")
    @patch("pyiron_base.cli.reloadfile.state")
    @patch("pyiron_base.cli.reloadfile.shutil")
    @patch("pyiron_base.cli.reloadfile._open_hdf")
    def test_main_strips_database_prefix_from_project_path(
        self, open_hdf_mock, shutil_mock, state_mock, project_mock
    ):
        expected_project_path = os.path.join(os.path.abspath("."), "toy_job.h5")
        db_top_path = os.path.dirname(expected_project_path)
        state_mock.database.top_path.return_value = db_top_path
        job_reload, project_instance = self._setup_mocks(
            open_hdf_mock, shutil_mock, state_mock, project_mock
        )

        parser = ArgumentParser()
        reloadfile.register(parser)
        args = parser.parse_args(["-i", "input/toy_job.h5", "-o", "output/toy_job.h5"])
        reloadfile.main(args)

        _, kwargs = project_instance.load_from_jobpath.call_args
        self.assertEqual(kwargs["db_entry"]["project"], "/")
        self.assertEqual(kwargs["db_entry"]["projectpath"], db_top_path)


if __name__ == "__main__":
    unittest.main()
