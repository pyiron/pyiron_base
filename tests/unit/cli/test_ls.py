# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import datetime
import unittest
from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

import pandas as pd

from pyiron_base.cli import ls


class TestLsCli(unittest.TestCase):
    def test_register_defaults(self):
        parser = ArgumentParser()
        ls.register(parser)
        args = parser.parse_args([])
        self.assertEqual(args.project, ".")
        self.assertFalse(args.recursive)
        self.assertEqual(args.name.pattern, "")
        self.assertIsNone(args.elements)
        self.assertIsNone(args.status)
        self.assertIsNone(args.since)
        self.assertEqual(
            args.columns,
            ["id", "status", "job", "timestart", "timestop", "totalcputime"],
        )
        self.assertFalse(args.all)

    def test_register_rejects_invalid_status(self):
        parser = ArgumentParser()
        ls.register(parser)
        with self.assertRaises(SystemExit):
            parser.parse_args(["-s", "not_a_status"])

    def test_register_name_is_compiled_regex(self):
        parser = ArgumentParser()
        ls.register(parser)
        args = parser.parse_args(["-n", "spx.*restart"])
        self.assertEqual(args.name.pattern, "spx.*restart")

    def _make_table(self):
        return pd.DataFrame(
            {
                "id": [1, 2],
                "status": ["finished", "aborted"],
                "job": ["job_a", "job_b"],
                "timestart": [
                    datetime.datetime.now() - datetime.timedelta(days=2),
                    datetime.datetime.now() - datetime.timedelta(days=2),
                ],
                "timestop": [
                    datetime.datetime.now() - datetime.timedelta(hours=1),
                    datetime.datetime.now() - datetime.timedelta(days=1),
                ],
                "totalcputime": [10.0, 20.0],
            }
        )

    @patch("pyiron_base.cli.ls.Project")
    def test_main_empty_table_exits_cleanly(self, project_mock):
        project = MagicMock()
        project.job_table.return_value = pd.DataFrame()
        project_mock.return_value = project

        parser = ArgumentParser()
        ls.register(parser)
        args = parser.parse_args([])

        with self.assertRaises(SystemExit) as ctx:
            ls.main(args)
        self.assertEqual(ctx.exception.code, 0)

    @patch("pyiron_base.cli.ls.Project")
    def test_main_passes_filters_to_job_table(self, project_mock):
        project = MagicMock()
        project.job_table.return_value = self._make_table()
        project_mock.return_value = project

        parser = ArgumentParser()
        ls.register(parser)
        args = parser.parse_args(["my_project", "-r", "-e", "Fe", "O"])
        ls.main(args)

        project_mock.assert_called_once_with("my_project")
        _, kwargs = project.job_table.call_args
        self.assertTrue(kwargs["recursive"])
        self.assertEqual(kwargs["element_lst"], ["Fe", "O"])

    @patch("pyiron_base.cli.ls.Project")
    def test_main_invalid_since_exits_with_error(self, project_mock):
        project = MagicMock()
        project.job_table.return_value = self._make_table()
        project_mock.return_value = project

        parser = ArgumentParser()
        ls.register(parser)
        args = parser.parse_args(["-i", "not-a-duration"])

        with self.assertRaises(SystemExit) as ctx:
            ls.main(args)
        self.assertEqual(ctx.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
