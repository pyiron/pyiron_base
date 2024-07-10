# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from pyiron_base.project.generic import Project
from pyiron_base.jobs.master.interactivewrapper import InteractiveWrapper
from pyiron_base._tests import PyironTestCase


class TestInteractiveWrapper(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_interactive"))

    def test_child_creation(self):
        """When creating an interactive wrapper from another job, that should be set as the wrapper's reference job."""
        j = self.project.create.job.ScriptJob("test_parent")
        j.server.run_mode = "interactive"
        i = j.create_job(InteractiveWrapper, "test_child")
        self.assertEqual(
            i.ref_job, j, "Reference job of interactive wrapper to set after creation."
        )
