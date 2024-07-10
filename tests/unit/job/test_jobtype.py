# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import PyironTestCase
from pyiron_base import JobType, GenericJob


old_job_type_dict = {
    "FlexibleMaster": "pyiron_base.jobs.master.flexible",
    "ScriptJob": "pyiron_base.jobs.script",
    "TableJob": "pyiron_base.jobs.datamining",
    "WorkerJob": "pyiron_base.jobs.worker",
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
                    cls = JobType.convert_str_to_class(
                        JobType._job_class_dict, job_type
                    )
                except AttributeError:
                    print(
                        f"Could not receive {job_type} class from {JobType._job_class_dict[job_type]}."
                    )
                    self.assertNotIn(job_type, old_job_type_dict)
                else:
                    if isinstance(cls, type):  # ignore dynamically defined classes
                        self.assertTrue(
                            issubclass(cls, GenericJob),
                            msg=f"{cls} is not a subclass of GenericJob",
                        )

    def test_register(self):
        job_class_list_no_error = [
            ("job1", "pyiron_module.sub_module"),
            ("job2", "pyiron_module.sub_module"),
            ("job3", "pyiron_module.sub_module"),
            ("job1", "pyiron_module.sub_module"),
            ("job_atomistics", "pyiron_atomistics.sub_module"),
            ("job_atomistics", "pyiron.sub_module"),
        ]
        for job_type in job_class_list_no_error:
            with self.subTest(job_type[1]):
                JobType.register(job_type[1], job_type[0])
                self.assertIn(job_type[0], JobType._job_class_dict)

        self.assertRaises(
            ValueError, JobType.register, ("pyiron_module.other_sub_module", "job1")
        )
        self.assertEqual(JobType._job_class_dict["job1"], "pyiron_module.sub_module")

        with self.subTest("overwrite"):
            JobType.register("pyiron_module.other_sub_module", "job1", overwrite=True)
            self.assertEqual(
                JobType._job_class_dict["job1"], "pyiron_module.other_sub_module"
            )

        # Cleanup
        for job in ["job1", "job2", "job3", "job_atomistics"]:
            JobType.unregister(job)

    def test_get_job_for_registered_class(self):
        from pyiron_base import Project, ProjectHDFio

        class Toy(GenericJob):
            pass

        self.assertNotIn("Toy", JobType._job_class_dict)
        job = JobType(Toy, ProjectHDFio(Project("."), "job_name"), "job_name")
        self.assertIsInstance(job, Toy)

        JobType.register("some.random.path", "Toy")

        with self.assertRaises(ValueError):
            job = JobType(Toy, ProjectHDFio(Project("."), "job_name"), "job_name")
