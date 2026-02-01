# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from pyiron_base.project.generic import Project
from pyiron_base._tests import TestWithProject


class TestCopyTo(TestWithProject):
    def test_copy_to_job(self):
        job = self.project.create.job.ScriptJob("job_script")

        with open("foo.py", "w") as f:
            f.write("print(42)\n")

        job.script_path = "foo.py"
        job.save()
        copy = job.copy_to(job.project_hdf5, "job_copy")
        self.assertEqual(
            job.script_path, copy.script_path, "Script path not equal after copy."
        )

        jobc = self.project.inspect(job.id)
        copyc = jobc.copy_to(jobc.project_hdf5, "job_core_copy")
        self.assertEqual(
            jobc.to_object().script_path,
            copyc.to_object().script_path,
            "Script path not equal after JobCore copy.",
        )

        os.remove("foo.py")

    def test_copy_to_project(self):
        sub_project = self.project.copy()
        sub_project = sub_project.open("sub_project")
        ham = self.project.create_job("ScriptJob", "job_single_pr")
        ham.copy_to(project=sub_project)

    def test_copy_to_project_ex(self):
        sub_project = self.project.copy()
        sub_project = sub_project.open("sub_project_ex")
        ham = self.project.create_job("ScriptJob", "job_single_pr_ex")
        ham.to_hdf()
        ham.copy_to(project=sub_project)
        ham.remove()
        os.remove(
            os.path.join(self.file_location, f"{sub_project.path}/job_single_pr_ex.h5")
        )

    def test_copy_to_name(self):
        """Regression test: copied jobs should have the updated name in the database.

        The was a bug where copied jobs would have the new name on the filesystem but still the old name in the
        database.
        """
        ham = self.project.create_job("ScriptJob", "copy_test")
        ham.save()

        new_ham = ham.copy_to(new_job_name="copy_test_new")
        self.assertEqual(new_ham.name, "copy_test_new", "New job has wrong name.")
        self.assertEqual(
            new_ham.name,
            new_ham.database_entry.job,
            "New job has wrong name in the database.",
        )

        ham.remove()
        new_ham.remove()


if __name__ == "__main__":
    unittest.main()
