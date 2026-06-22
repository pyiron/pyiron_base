# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from argparse import ArgumentParser
from unittest.mock import patch

from pyiron_base.cli import install


class TestInstallCli(unittest.TestCase):
    def test_register_defaults(self):
        parser = ArgumentParser()
        install.register(parser)
        args = parser.parse_args([])
        self.assertEqual(args.config, "~/.pyiron")
        self.assertEqual(args.resources, "~/pyiron/resources")
        self.assertEqual(args.project, "~/pyiron/projects")
        self.assertTrue(args.url.startswith("https://"))

    def test_register_explicit(self):
        parser = ArgumentParser()
        install.register(parser)
        args = parser.parse_args(
            [
                "-c",
                "my_config",
                "-r",
                "my_resources",
                "-u",
                "http://example.com/resources.tar.gz",
                "-p",
                "my_projects",
            ]
        )
        self.assertEqual(args.config, "my_config")
        self.assertEqual(args.resources, "my_resources")
        self.assertEqual(args.url, "http://example.com/resources.tar.gz")
        self.assertEqual(args.project, "my_projects")

    @patch("pyiron_base.cli.install.install_pyiron")
    def test_main(self, install_pyiron_mock):
        parser = ArgumentParser()
        install.register(parser)
        args = parser.parse_args(
            [
                "-c",
                "my_config",
                "-r",
                "my_resources",
                "-u",
                "http://example.com/resources.tar.gz",
                "-p",
                "my_projects",
            ]
        )
        install.main(args)

        install_pyiron_mock.assert_called_once_with(
            config_file_name="my_config",
            project_path="my_projects",
            resource_directory="my_resources",
            giturl_for_zip_file="http://example.com/resources.tar.gz",
        )


if __name__ == "__main__":
    unittest.main()
