# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

from pyiron_base.cli import mv


class TestMvCli(unittest.TestCase):
    def test_register(self):
        parser = ArgumentParser()
        mv.register(parser)
        args = parser.parse_args(["source_project", "destination_project"])
        self.assertEqual(args.src, "source_project")
        self.assertEqual(args.dst, "destination_project")

    @patch("pyiron_base.cli.mv.Project")
    def test_main(self, project_mock):
        src_project = MagicMock()
        dst_project = MagicMock()
        project_mock.side_effect = [src_project, dst_project]

        parser = ArgumentParser()
        mv.register(parser)
        args = parser.parse_args(["source_project", "destination_project"])
        mv.main(args)

        project_mock.assert_any_call("source_project")
        project_mock.assert_any_call("destination_project")
        src_project.move_to.assert_called_once_with(dst_project)


if __name__ == "__main__":
    unittest.main()
