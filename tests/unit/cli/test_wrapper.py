# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from argparse import ArgumentParser
from unittest.mock import patch

from pyiron_base.cli import wrapper


class TestWrapperCli(unittest.TestCase):
    def test_register(self):
        parser = ArgumentParser()
        wrapper.register(parser)
        args = parser.parse_args(
            [
                "-d",
                "-j",
                "42",
                "-p",
                "my_project",
                "-f",
                "my_file.h5",
                "-s",
                "-c",
            ]
        )
        self.assertTrue(args.debug)
        self.assertEqual(args.job_id, "42")
        self.assertEqual(args.project, "my_project")
        self.assertEqual(args.file_path, "my_file.h5")
        self.assertTrue(args.submit)
        self.assertTrue(args.collect)

    def test_register_defaults(self):
        parser = ArgumentParser()
        wrapper.register(parser)
        args = parser.parse_args([])
        self.assertFalse(args.debug)
        self.assertIsNone(args.job_id)
        self.assertIsNone(args.project)
        self.assertIsNone(args.file_path)
        self.assertFalse(args.submit)
        self.assertFalse(args.collect)

    @patch("pyiron_base.cli.wrapper.job_wrapper_function")
    def test_main(self, job_wrapper_function_mock):
        parser = ArgumentParser()
        wrapper.register(parser)
        args = parser.parse_args(
            ["-d", "-j", "42", "-p", "my_project", "-f", "my_file.h5", "-s", "-c"]
        )
        wrapper.main(args)

        job_wrapper_function_mock.assert_called_once_with(
            working_directory="my_project",
            job_id="42",
            file_path="my_file.h5",
            debug=True,
            submit_on_remote=True,
            collect=True,
        )


if __name__ == "__main__":
    unittest.main()
