# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import PyironTestCase
from pyiron_base import JobType


class TestJobType(PyironTestCase):
    def test_job_class_dict(self):
        job_dict = JobType._job_class_dict

        old_job_type_dict = {
            "FlexibleMaster": "pyiron_base.master.flexible",
            "ScriptJob": "pyiron_base.job.script",
            "SerialMasterBase": "pyiron_base.master.serial",
            "TableJob": "pyiron_base.table.datamining",
            "WorkerJob": "pyiron_base.job.worker",
        }

        for key in old_job_type_dict:
            self.assertIn(key, job_dict)
            self.assertEqual(job_dict[key], old_job_type_dict[key])

        excluded_jobs = [
            "ListMaster",
            "ParallelMaster",
            "InteractiveBase",
            "InteractiveWrapper",
            "TemplateJob",
            "PythonTemplateJob",
            "GenericMaster",
        ]
        for key in excluded_jobs:
            with self.subTest(key):
                self.assertNotIn(key, old_job_type_dict)
