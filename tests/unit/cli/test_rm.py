# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

from pyiron_base.cli import rm


class TestRmCli(unittest.TestCase):
    def test_register_defaults(self):
        parser = ArgumentParser()
        rm.register(parser)
        args = parser.parse_args([])
        self.assertEqual(args.project, ".")
        self.assertFalse(args.jobs_only)
        self.assertFalse(args.recursive)

    def test_register_explicit(self):
        parser = ArgumentParser()
        rm.register(parser)
        args = parser.parse_args(["my_project", "-j", "-r"])
        self.assertEqual(args.project, "my_project")
        self.assertTrue(args.jobs_only)
        self.assertTrue(args.recursive)

    @patch("pyiron_base.cli.rm.os")
    @patch("pyiron_base.cli.rm.Project")
    def test_main_jobs_only(self, project_mock, os_mock):
        project = MagicMock()
        project_mock.return_value = project

        parser = ArgumentParser()
        rm.register(parser)
        args = parser.parse_args(["my_project", "-j", "-r"])
        rm.main(args)

        project_mock.assert_called_once_with("my_project")
        project.remove_jobs.assert_called_once_with(recursive=True, silently=True)
        project.remove.assert_not_called()

    @patch("pyiron_base.cli.rm.os")
    @patch("pyiron_base.cli.rm.Project")
    def test_main_remove_project_and_empty_dir(self, project_mock, os_mock):
        project = MagicMock()
        project_mock.return_value = project
        os_mock.path.exists.return_value = True
        os_mock.listdir.return_value = []

        parser = ArgumentParser()
        rm.register(parser)
        args = parser.parse_args(["my_project"])
        rm.main(args)

        project.remove.assert_called_once_with(enable=True)
        os_mock.rmdir.assert_called_once_with("my_project")

    @patch("pyiron_base.cli.rm.os")
    @patch("pyiron_base.cli.rm.Project")
    def test_main_remove_project_nonempty_dir_not_removed(
        self, project_mock, os_mock
    ):
        project = MagicMock()
        project_mock.return_value = project
        os_mock.path.exists.return_value = True
        os_mock.listdir.return_value = ["leftover_file"]

        parser = ArgumentParser()
        rm.register(parser)
        args = parser.parse_args(["my_project"])
        rm.main(args)

        project.remove.assert_called_once_with(enable=True)
        os_mock.rmdir.assert_not_called()


if __name__ == "__main__":
    unittest.main()
