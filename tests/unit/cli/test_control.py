# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from unittest.mock import patch

from pyiron_base.cli import control


class TestControlCli(unittest.TestCase):
    def test_cli_modules_registered(self):
        expected = {"cp", "ls", "mv", "rm", "install", "reloadfile", "wrapper"}
        self.assertEqual(set(control.cli_modules.keys()), expected)

    def test_no_subcommand_errors(self):
        with patch("sys.argv", ["pyiron"]):
            with self.assertRaises(SystemExit):
                control.main()

    @patch("pyiron_base.cli.control.os")
    def test_dispatches_to_subcommand(self, os_mock):
        os_mock.path.exists.return_value = False
        called = {}

        def fake_main(args):
            called["args"] = args

        with patch.dict(control.cli_modules, {"cp": control.cli_modules["cp"]}):
            with patch.object(control.cli_modules["cp"], "main", fake_main):
                with patch("sys.argv", ["pyiron", "cp", "src_project", "dst_project"]):
                    control.main()

        self.assertEqual(called["args"].src, "src_project")
        self.assertEqual(called["args"].dst, "dst_project")

    @patch("pyiron_base.cli.control.os")
    def test_removes_log_file_unless_dirty(self, os_mock):
        os_mock.path.exists.return_value = True

        with patch.object(control.cli_modules["cp"], "main", lambda args: None):
            with patch("sys.argv", ["pyiron", "cp", "src_project", "dst_project"]):
                control.main()
        os_mock.remove.assert_called_once_with("pyiron.log")

    @patch("pyiron_base.cli.control.os")
    def test_dirty_flag_keeps_log_file(self, os_mock):
        os_mock.path.exists.return_value = True

        with patch.object(control.cli_modules["cp"], "main", lambda args: None):
            with patch(
                "sys.argv", ["pyiron", "-d", "cp", "src_project", "dst_project"]
            ):
                control.main()
        os_mock.remove.assert_not_called()


if __name__ == "__main__":
    unittest.main()
