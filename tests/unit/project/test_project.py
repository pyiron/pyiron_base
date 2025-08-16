# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, join, abspath, exists, islink
import os
import tempfile
import pickle
import shutil
from pyiron_base.project.generic import Project
from pyiron_base._tests import (
    PyironTestCase,
    TestWithProject,
    TestWithFilledProject,
    ToyJob,
)
from pyiron_base.jobs.job.toolkit import BaseTools


try:
    import pint

    from pyiron_base.project.size import _size_conversion

    pint_not_available = False
except ImportError:
    pint_not_available = True


try:
    import git

    git_not_available = False
except ImportError:
    git_not_available = True


class TestProjectData(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass

    def setUp(self):
        super().setUp()
        self.project = Project(self.project_name)

    def tearDown(self):
        self.project.remove(enable=True)

    def test_data(self):
        self.assertRaises(KeyError, self.project.data.read)

        self.project.data.foo = "foo"
        self.project.data.write()
        self.project.data.read()
        self.assertEqual(1, len(self.project.data))

        project2 = Project(self.project_name)
        self.assertEqual(1, len(project2.data))
        self.assertEqual(self.project.data.foo, project2.data.foo)

    def test_iterjobs(self):
        try:
            for j in self.project.iter_jobs():
                pass
        except:
            self.fail("Iterating over empty project should not raise exception.")

        try:
            for j in self.project.iter_jobs(status="finished"):
                pass
        except:
            self.fail(
                "Iterating over empty project with set status flag should not raise exception."
            )

    def test_create_job_name(self):
        self.assertEqual(self.project.create.job_name(["job", 0.1]), "job_0d1")
        self.assertEqual(
            self.project.create.job_name(["job", 0.1], special_symbols={".": "c"}),
            "job_0c1",
        )
        self.assertEqual(
            self.project.create.job_name(["job", 1.0000000000005]), "job_1d0"
        )
        self.assertEqual(
            self.project.create.job_name(["job", 1.0000000000005], ndigits=None),
            "job_1d0000000000005",
        )


class TestProjectMove(TestWithFilledProject):
    def test_copy_to(self):
        with self.subTest("copy_to destination project"):
            # cp project  destination creating a reference_pr
            destination_pr = self.project.parent_group.open("destination")
            self.assertEqual(len(destination_pr.list_groups()), 0)
            self.assertEqual(len(destination_pr.list_nodes()), 0)
            self.assertEqual(len(destination_pr.list_files()), 0)
            self.project.copy_to(destination_pr)
            self.assertEqual(self.project.list_groups(), destination_pr.list_groups())
            self.assertEqual(self.project.list_nodes(), destination_pr.list_nodes())
            self.assertEqual(self.project.list_files(), destination_pr.list_files())
            reference_pr = destination_pr
        with self.subTest("copy_to with delete_original_data"):
            destination_pr = self.project.parent_group.open("destination2")
            self.assertEqual(len(destination_pr.list_groups()), 0)
            self.assertEqual(len(destination_pr.list_nodes()), 0)
            self.assertEqual(len(destination_pr.list_files()), 0)
            self.project.copy_to(destination_pr, delete_original_data=True)
            self.assertEqual(reference_pr.list_groups(), destination_pr.list_groups())
            self.assertEqual(reference_pr.list_nodes(), destination_pr.list_nodes())
            self.assertEqual(reference_pr.list_files(), destination_pr.list_files())
            self.assertFalse(os.path.exists(self.project_path))
        with self.subTest("destination2 move_to old project"):
            destination_pr.move_to(self.project)
            self.assertEqual(self.project.list_groups(), reference_pr.list_groups())
            self.assertEqual(self.project.list_nodes(), reference_pr.list_nodes())
            self.assertEqual(self.project.list_files(), reference_pr.list_files())
            self.assertFalse(os.path.exists(destination_pr.path))
        with self.subTest("copy_to with project data"):
            destination_pr = self.project.parent_group.open("destination3")
            self.project.data["foo"] = 42
            self.project.data.write()
            self.project.copy_to(destination_pr)
            self.assertEqual(self.project.list_groups(), destination_pr.list_groups())
            self.assertEqual(self.project.list_nodes(), destination_pr.list_nodes())
            self.assertEqual(self.project.list_files(), destination_pr.list_files())
            destination_pr.data.read()
            self.assertEqual(destination_pr.data["foo"], 42)
            destination_pr.remove(enable=True)


class TestProjectOperations(TestWithFilledProject):
    @unittest.skipIf(
        pint_not_available,
        "pint is not installed so the pint related tests are skipped.",
    )
    def test_size(self):
        self.assertTrue(self.project.size > 0)

    @unittest.skipIf(
        pint_not_available,
        "pint is not installed so the pint related tests are skipped.",
    )
    def test__size_conversion(self):
        conv_check = {
            -2000: (-1.953125, "kibibyte"),
            0: (0, "byte"),
            50: (50, "byte"),
            2000: (1.953125, "kibibyte"),
            2**20: (1.0, "mebibyte"),
            2**30: (1.0, "gibibyte"),
            2**40: (1.0, "tebibyte"),
            2**50: (1.0, "pebibyte"),
            2**60: (1024.0, "pebibyte"),
        }

        byte = pint.UnitRegistry().byte
        for value in conv_check.keys():
            with self.subTest(value):
                converted_size = _size_conversion(value * byte)
                self.assertEqual(converted_size.m, conv_check[value][0])
                self.assertEqual(str(converted_size.u), conv_check[value][1])

    def test_job_table(self):
        df = self.project.job_table()
        self.assertEqual(len(df), self.n_jobs_filled_with)
        self.assertEqual(
            " ".join(df.status.sort_values().unique()), "aborted finished suspended"
        )

    def test_filtered_job_table(self):
        # Here we test against the filled project
        # Counts should match values there, but since we're testing our ability to count
        # we can't just dynamically fill these values.
        # Thus, these are read an inferred (mostly) by hand from TestWithFilledProject
        n_jobs = self.n_jobs_filled_with
        n_finished_jobs = 3
        n_aborted_jobs = 1
        n_suspended_jobs = 1
        # n_ToyJobs = 5
        n_jobs_named_toy_1 = 2
        # And for when recursive=False:
        n_top_jobs = 2
        n_top_finished_jobs = 1
        # n_top_aborted_jobs = 1
        n_top_suspended_jobs = 0
        # n_top_ToyJobs = 2
        n_top_jobs_named_toy_1 = 1

        self.assertEqual(
            len(self.project.job_table(recursive=False)),
            n_top_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True)),
            n_jobs,
            msg="Expected to find all the jobs the project was filled with",
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, status="finished")),
            n_finished_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=False, status="finished")),
            n_top_finished_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, status="suspended")),
            n_suspended_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, status="aborted")),
            n_aborted_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=False, status="suspended")),
            n_top_suspended_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=False, hamilton="ToyJob")), n_top_jobs
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, parentid=None)), n_jobs
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, status="finished", job="toy_1")),
            n_jobs_named_toy_1,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, job="toy*")), n_jobs
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, job="*_1*")), n_jobs_named_toy_1
        )
        self.assertEqual(len(self.project.job_table(recursive=True, job="*_*")), n_jobs)
        self.assertEqual(
            len(
                self.project.job_table(recursive=False, status="finished", job="toy_1")
            ),
            n_top_jobs_named_toy_1,
        )
        self.assertEqual(
            len(
                self.project.job_table(
                    recursive=True, status="^(?!finished)", mode="regex"
                )
            ),
            n_suspended_jobs + n_aborted_jobs,
        )
        self.assertEqual(
            len(
                self.project.job_table(
                    recursive=True, status="^(?!aborted)", mode="regex"
                )
            ),
            n_jobs - n_aborted_jobs,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, job="^(?!toy_1)", mode="regex")),
            n_jobs - n_jobs_named_toy_1,
        )
        self.assertEqual(
            len(self.project.job_table(recursive=True, job="^(?!toy_)", mode="regex")),
            0,
        )
        self.assertRaises(ValueError, self.project.job_table, gibberish=True)

    def test_get_iter_jobs(self):
        self.assertEqual(
            [
                job.output.data_out
                for job in self.project.iter_jobs(
                    recursive=True, convert_to_object=True
                )
            ],
            [101] * self.n_jobs_filled_with,
        )
        self.assertEqual(
            [
                val
                for val in self.project.iter_jobs(recursive=False, status="suspended")
            ],
            [],
        )
        self.assertIsInstance(
            [
                val
                for val in self.project.iter_jobs(
                    recursive=True, status="suspended", convert_to_object=True
                )
            ][0],
            ToyJob,
        )

    def test_iter_jobs_without_database(self):
        pr = Project("test_iter_jobs_without_database")
        database_disabled = pr.state.settings.configuration["disable_database"]
        pr.state.update({"disable_database": True})

        pr_2 = pr.open("sub_1")
        job_1 = pr_2.create_job(ToyJob, "toy1")
        job_1.run()

        pr_3 = pr.open("sub_2")
        job_1 = pr_3.create_job(ToyJob, "toy2")
        job_1.run()

        try:
            job_names = []
            for job in pr.iter_jobs(status="finished"):
                job_names.append(job.job_name)
            self.assertListEqual(
                ["toy1", "toy2"],
                job_names,
                msg="Expected to iterate among nested projects even without database",
            )
        finally:
            pr.remove_jobs(recursive=True, silently=True)
            pr.remove(enable=True)
            pr.state.update({"disable_database": database_disabled})

    @unittest.skipIf(
        git_not_available,
        "gitpython is not available so the gitpython related tests are skipped.",
    )
    def test_maintenance_get_repository_status(self):
        df = self.project.maintenance.get_repository_status()
        self.assertIn("pyiron_base", df.Module.values)

    def test_pickle(self):
        """Project should be pickle-able."""
        project_reload = pickle.loads(pickle.dumps(self.project))
        self.assertEqual(project_reload.root_path, self.project.root_path)
        self.assertEqual(project_reload.project_path, self.project.project_path)


@unittest.skipUnless(
    os.name == "posix", "symlinking is only available on posix platforms"
)
class TestProjectSymlink(TestWithFilledProject):
    """
    Test that Project.symlink creates a symlink and unlink removes it again.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.temp = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.temp.cleanup()

    def test_symlink(self):
        nodes = self.project.list_nodes()
        groups = self.project.list_groups()

        self.project.symlink(self.temp.name)

        try:
            self.project.symlink(self.temp.name)
        except Exception as e:
            self.fail(f"symlinking twice should have no effect, but raised {e}!")

        with self.assertRaises(
            RuntimeError, msg="symlinking to another folder should raise an error"
        ):
            self.project.symlink("asdf")

        path = self.project.path.rstrip(os.sep)
        self.assertTrue(islink(path), "symlink() did not create a symlink!")
        self.assertEqual(
            os.readlink(path),
            join(self.temp.name, self.project.name),
            "symlink() created a wrong symlink!",
        )

        self.assertCountEqual(
            nodes, self.project.list_nodes(), "not all nodes present after symlink!"
        )
        self.assertCountEqual(
            groups, self.project.list_groups(), "not all groups present after symlink!"
        )

        self.project.unlink()

        self.assertTrue(
            exists(self.project.path), "unlink() did not restore original directory!"
        )
        self.assertFalse(islink(path), "unlink() did not remove symlink!")

        self.assertCountEqual(
            nodes, self.project.list_nodes(), "not all nodes present after unlink!"
        )
        self.assertCountEqual(
            groups, self.project.list_groups(), "not all groups present after unlink!"
        )

        try:
            self.project.unlink()
        except Exception as e:
            self.fail(f"unlinking twice should have no effect, but raised {e}!")


class TestToolRegistration(TestWithProject):
    def setUp(self) -> None:
        super().setUp()
        self.tools = BaseTools(self.project)

    def test_registration(self):
        self.project.register_tools("foo", self.tools)
        with self.assertRaises(AttributeError):
            self.project.register_tools("foo", self.tools)  # Name taken
        with self.assertRaises(AttributeError):
            self.project.register_tools("load", self.tools)  # Already another method


class TestProjectExtended(TestWithProject):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project_extended")
        if os.path.exists(cls.project_name):
            shutil.rmtree(cls.project_name)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass
        super().tearDownClass()

    def test_init_with_default_working_directory(self):
        # Test case where path is empty and default_working_directory is True
        # and Notebook.get_custom_dict() returns a project_dir
        try:
            from pyiron_base.project.external import Notebook
            original_get_custom_dict = Notebook.get_custom_dict

            class MockNotebook:
                @staticmethod
                def get_custom_dict():
                    return {"project_dir": self.project_path}

            Notebook.get_custom_dict = MockNotebook.get_custom_dict

            pr = Project(default_working_directory=True)
            self.assertEqual(pr.path, self.project_path + "/")
        finally:
            Notebook.get_custom_dict = original_get_custom_dict

        # Test case where path is empty and default_working_directory is True
        # and Notebook.get_custom_dict() returns None
        try:
            from pyiron_base.project.external import Notebook
            original_get_custom_dict = Notebook.get_custom_dict

            class MockNotebook:
                @staticmethod
                def get_custom_dict():
                    return None

            Notebook.get_custom_dict = MockNotebook.get_custom_dict

            pr = Project(default_working_directory=True)
            self.assertEqual(os.path.abspath(pr.path), os.path.abspath("."))
        finally:
            Notebook.get_custom_dict = original_get_custom_dict


if __name__ == "__main__":
    unittest.main()
