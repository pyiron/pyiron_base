# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import shutil
from pyiron_base.state.install import install_pyiron
from pyiron_base._tests import PyironTestCase


class TestInstall(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.execution_path = os.path.dirname(os.path.abspath(__file__))

    @classmethod
    def tearDownClass(cls):
        execution_path = os.path.dirname(os.path.abspath(__file__))
        shutil.rmtree(os.path.join(execution_path, "resources"))
        shutil.rmtree(os.path.join(execution_path, "project"))
        os.remove(os.path.join(execution_path, "config"))
        try:
            os.remove(os.path.join(execution_path, "pyiron.log"))
        except FileNotFoundError:
            pass

    def test_install(self):
        install_pyiron(
            config_file_name=os.path.join(self.execution_path, "config"),
            resource_directory=os.path.join(self.execution_path, "resources"),
            project_path=os.path.join(self.execution_path, "project"),
            giturl_for_zip_file=None,
        )

        with open(os.path.join(self.execution_path, "config"), "r") as f:
            content = f.readlines()
        self.assertEqual(content[0], "[DEFAULT]\n")
        self.assertIn("PROJECT_PATHS", content[1])
        self.assertIn("RESOURCE_PATHS", content[2])
        self.assertTrue(os.path.exists(os.path.join(self.execution_path, "project")))
        self.assertTrue(os.path.exists(os.path.join(self.execution_path, "resources")))
