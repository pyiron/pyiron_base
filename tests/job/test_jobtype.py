# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import PyironTestCase
from pyiron_base import JobType, GenericJob


old_job_type_dict = {
    "FlexibleMaster": "pyiron_base.master.flexible",
    "ScriptJob": "pyiron_base.job.script",
    "SerialMasterBase": "pyiron_base.master.serial",
    "TableJob": "pyiron_base.table.datamining",
    "WorkerJob": "pyiron_base.job.worker",
}


class TestJobType(PyironTestCase):
    def test_job_class_dict(self):
        job_dict = JobType._job_class_dict

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
                self.assertNotIn(key, job_dict)

    def test_convert_str_to_class(self):
        for job_type in JobType._job_class_dict:
            with self.subTest(job_type):
                try:
                    cls = JobType.convert_str_to_class(JobType._job_class_dict, job_type)
                except AttributeError:
                    print(f"Could not receive {job_type} class from {JobType._job_class_dict[job_type]}.")
                    self.assertNotIn(job_type, old_job_type_dict)
                else:
                    self.assertTrue(
                        issubclass(cls, GenericJob),
                        msg=f"{cls} is not a subclass of GenericJob",
                    )
