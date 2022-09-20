# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.storage.parameters import GenericParameters
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base._tests import TestWithFilledProject, ToyJob


class ReturnCodeJob(GenericJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.input = GenericParameters(table_name="input")
        self.input["return_code"] = 0
        self.input["accepted_codes"] = []
        self.executable = f"exit {self.input['return_code']}"

    def write_input(self):
        self.executable = f"exit {self.input['return_code']}"
        self.executable.accepted_return_codes += self.input["accepted_codes"]

    def collect_output(self):
        pass

class TestGenericJob(TestWithFilledProject):
    def test_db_entry(self):
        ham = self.project.create.job.ScriptJob("job_single_debug")
        db_entry = ham.db_entry()
        self.assertEqual(db_entry["project"], ham.project_hdf5.project_path)
        self.assertEqual(db_entry["hamilton"], "Script")
        self.assertEqual(db_entry["hamversion"], ham.version)
        self.assertEqual(db_entry["status"], ham.status.string)
        self.assertEqual(db_entry["job"], ham.job_name)
        ham.save()
        ham.remove()

    def test_reload_empty_job(self):
        job_empty = self.project.create_job(
            job_type=GenericJob,
            job_name="empty_reload"
        )
        job_id = job_empty.project.db.add_item_dict(job_empty.db_entry())
        job_empty_inspect = self.project.inspect(job_id)
        self.assertEqual(len(job_empty_inspect.list_nodes()), 0)
        self.assertTrue(job_empty_inspect.project_hdf5.is_empty)
        with self.assertRaises(ValueError):
            self.project.load(job_id)

    def test_id(self):
        pass

    def test_parent_id(self):
        pass

    def test_master_id(self):
        pass

    def test_child_ids(self):
        pass

    def test_child_ids_running(self):
        pass

    def test_child_ids_finished(self):
        pass

    def test_index(self):
        pass

    def test_job_name(self):
        cwd = self.file_location
        with self.subTest("ensure create is working"):
            ham = self.project.create.job.ScriptJob("job_single_debug")
            self.assertEqual("job_single_debug", ham.job_name)
            self.assertEqual("/job_single_debug", ham.project_hdf5.h5_path)
            self.assertEqual("/".join([cwd, self.project_name, "job_single_debug.h5"]), ham.project_hdf5.file_name)
            self.assertEqual(
                "/".join([cwd, self.project_name, "job_single_debug_hdf5/job_single_debug"]),
                ham.working_directory
            )
        with self.subTest("test move"):
            ham.job_name = "job_single_move"
            ham.to_hdf()
            self.assertEqual("/job_single_move", ham.project_hdf5.h5_path)
            self.assertEqual("/".join([cwd, self.project_name, "job_single_move.h5"]), ham.project_hdf5.file_name)
            self.assertEqual(
                "/".join([cwd, self.project_name, "job_single_move_hdf5/job_single_move"]),
                ham.working_directory
            )
            self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
            ham.project_hdf5.create_working_directory()
            self.assertTrue(os.path.exists(ham.working_directory))
        with self.subTest("test remove"):
            ham.project_hdf5.remove_file()
            self.assertFalse(os.path.isfile(ham.project_hdf5.file_name))

        with self.subTest('ensure create is working'):
            ham = self.project.create.job.ScriptJob("job_single_debug_2")
            ham.to_hdf()
            self.assertEqual("job_single_debug_2", ham.job_name)
            self.assertEqual("/job_single_debug_2", ham.project_hdf5.h5_path)
            self.assertEqual("/".join([cwd, self.project_name, "job_single_debug_2.h5"]), ham.project_hdf5.file_name)
            self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
        with self.subTest('Add files to working directory'):
            ham.project_hdf5.create_working_directory()
            self.assertEqual(ham.list_files(), ["WARN_pyiron_modified_content"])
            self.assertEqual(
                "/".join([cwd, self.project_name, "job_single_debug_2_hdf5/job_single_debug_2"]),
                ham.working_directory
            )
            self.assertTrue(os.path.exists(ham.working_directory))
            with open(os.path.join(ham.working_directory, 'test_file'), 'w') as f:
                f.write("Content")
            self.assertEqual(
                ham.list_files(), ["test_file", "WARN_pyiron_modified_content"]
            )
        with self.subTest("Compress"):
            ham.compress()
            self.assertFalse(os.path.exists(os.path.join(ham.working_directory, 'test_file')))
            self.assertTrue(os.path.exists(os.path.join(ham.working_directory, ham.job_name + '.tar.bz2')))
        with self.subTest("Decompress"):
            ham.decompress()
            self.assertTrue(os.path.exists(os.path.join(ham.working_directory, 'test_file')))
            ham.compress()
        with self.subTest("test move"):
            ham.job_name = "job_single_move_2"
            self.assertEqual(
                "/".join([cwd, self.project_name, "job_single_move_2_hdf5/job_single_move_2"]),
                ham.working_directory
            )
            self.assertFalse(os.path.exists(
                "/".join([cwd, self.project_name, "job_single_debug_2_hdf5/job_single_debug_2"])
            ))
            self.assertTrue(os.path.exists(os.path.join(ham.working_directory, ham.job_name + '.tar.bz2')),
                            msg="Job compressed archive not renamed.")
            self.assertTrue(os.path.exists(ham.working_directory))
            self.assertEqual("/job_single_move_2", ham.project_hdf5.h5_path)
            self.assertEqual("/".join([cwd, self.project_name, "job_single_move_2.h5"]), ham.project_hdf5.file_name)
            self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
        with self.subTest("Decompress 2"):
            ham.decompress()
            self.assertTrue(os.path.exists(os.path.join(ham.working_directory, 'test_file')))
        with self.subTest("test remove"):
            self.assertTrue(os.path.isfile("/".join([cwd, self.project_name, "job_single_move_2.h5"])))
            ham.project_hdf5.remove_file()
            self.assertFalse(os.path.isfile("/".join([cwd, self.project_name, "job_single_move_2.h5"])))
            self.assertFalse(os.path.isfile(ham.project_hdf5.file_name))
            self.assertTrue(os.path.exists(os.path.join(ham.working_directory, 'test_file')))
            ham.remove()
            self.assertFalse(os.path.exists(os.path.join(ham.working_directory, 'test_file')))

    def test_move(self):
        pr_a = self.project.open("project_a")
        pr_b = self.project.open("project_b")
        ham = pr_a.create.job.ScriptJob("job_moving_easy")
        self.assertFalse(ham.project_hdf5.file_exists)
        self.assertTrue(self.project_name + "/project_a/" in ham.project_hdf5.project_path)
        self.assertFalse(ham.project_hdf5.file_exists)
        ham.move_to(pr_b)
        self.assertTrue(self.project_name + "/project_b/" in ham.project_hdf5.project_path)
        ham_2 = pr_a.create.job.ScriptJob("job_moving_diff")
        ham_2.to_hdf()
        self.assertTrue(self.project_name + "/project_a/" in ham_2.project_hdf5.project_path)
        ham_2.move_to(pr_b)
        self.assertTrue(self.project_name + "/project_b/" in ham_2.project_hdf5.project_path)
        ham_2.project_hdf5.remove_file()
        pr_a.remove(enable=True)
        pr_b.remove(enable=True)

    def test_copy_to(self):
        job = self.project.create.job.ScriptJob("template")
        job.save()
        job_copy = job.copy_to(new_job_name="template_copy", input_only=False, new_database_entry=False)
        job_copy.save()
        job_copy.status.finished = True
        df = self.project.job_table()
        self.assertEqual("template", sorted(df.job.values)[0])
        self.assertEqual("template_copy", sorted(df.job.values)[1])
        # Load job if copied with same name
        job_copy_again = job.copy_to(new_job_name="template_copy", input_only=False, new_database_entry=False)
        self.assertEqual(job_copy["input/generic_dict"], job_copy_again["input/generic_dict"])
        self.assertTrue(job_copy_again.status.finished)
        # Completely new job name
        job_new = job.copy_to(new_job_name="template_new", input_only=False, new_database_entry=False)
        self.assertTrue(job_new.status.initialized)
        # Check if new database entry implemented
        _ = job.copy_to(new_job_name="template_last", input_only=False, new_database_entry=True)
        df = self.project.job_table()
        self.assertEqual(len(df[df.job == "template"]), 2)
        _ = job.copy_to(new_job_name="template_copy", input_only=False, new_database_entry=False,
                        delete_existing_job=True)
        df = self.project.job_table()
        self.assertTrue("template_copy" not in df.job.values)
        # Check that new name and new project can both be provided at once
        parent_job = self.project.create.job.ScriptJob('parent')
        new_job_name = 'parents_child'
        job_copy = job.copy_template(project=parent_job._hdf5, new_job_name=new_job_name)
        self.assertEqual(job_copy.project_hdf5.path.split('/')[-2], parent_job.job_name)
        self.assertEqual(job_copy.job_name, new_job_name)

    # def test_sub_job_name(self):
    #     pass

    def test_version(self):
        pass

    def test_structure(self):
        pass

    def test_executable(self):
        pass

    def test_project(self):
        pass

    def test_hdf5(self):
        pass

    def test_server(self):
        pass

    def test_status(self):
        pass

    def test_job_info_str(self):
        pass

    def test_write_input(self):
        pass

    def test_collect_output(self):
        pass

    def test_collect_logfiles(self):
        pass

    def test_run(self):
        pass

    def test_run_if_modal(self):
        pass

    def test_run_if_non_modal(self):
        pass

    def test_run_if_manually(self):
        pass

    def test_run_if_queue(self):
        pass

    def test_run_if_new(self):
        pass

    def test_run_if_appended(self):
        pass

    def test_run_if_created(self):
        pass

    def test_run_if_submitted(self):
        pass

    def test_run_if_running(self):
        pass

    def test_run_if_refresh(self):
        pass

    def test_run_if_collect(self):
        pass

    def test_run_if_suspended(self):
        pass

    def test_run_if_finished(self):
        pass

    def test_run_with_delete_existing_job_for_aborted_jobs(self):
        job = self.project.create_job(ToyJob, 'rerun_aborted')
        with self.subTest("Drop to aborted if validate_ready_to_run fails"):
            job.input.data_in = 'some_str'
            self.assertRaises(ValueError, job.run)
            self.assertTrue(job.status.aborted)
            self.assertIsNone(job.job_id)
        with self.subTest("run without delete_existing_job raises a RuntimeError."):
            self.assertRaises(ValueError, job.run)
            self.assertTrue(job.status.aborted)
        with self.subTest("changing input and run(delete_existing_job=True) should run"):
            job.input.data_in = 10
            job.run(delete_existing_job=True)
            self.assertIsInstance(job.job_id, int)
            self.assertEqual(job.output.data_out, 11)
            self.assertTrue(job.status.finished)
        with self.subTest("changing input and run(delete_existing_job=True) should run also for finished jobs"):
            job.input.data_in = 15
            job.run(delete_existing_job=True)
            self.assertEqual(job.output.data_out, 16)
            self.assertTrue(job.status.finished)

    def test_suspend(self):
        pass

    def test_remove(self):
        pass

    def test_inspect(self):
        pass

    def test_load(self):
        pass

    def test_save(self):
        pass

    def test_job_file_name(self):
        pass

    def test_clear_job(self):
        pass

    def test_rename(self):
        pass

    def test_rename_nested_job(self):
        pass

    def test_copy(self):
        pass

    def test_update_master(self):
        pass

    def test_to_from_hdf(self):
        pass

    def test_to_from_hdf_serial(self):
        pass

    def test_to_from_hdf_childshdf(self):
        pass

    def test_show_hdf(self):
        pass

    def test_get_pandas(self):
        pass

    def test_get_from_table(self):
        pass

    def test_get(self):
        pass

    def test_append(self):
        pass

    def test_pop(self):
        pass

    def test_iter_childs(self):
        pass

    def test__check_if_job_exists(self):
        pass

    def test__runtime(self):
        pass

    def test__create_working_directory(self):
        wd_warn_key = "write_work_dir_warnings"
        previous_wd_warn_setting = self.project.state.settings.configuration[
            wd_warn_key
        ]
        try:
            with self.subTest("Writing warning file"):
                self.project.state.settings.configuration[wd_warn_key] = True
                job = self.project.create_job(ToyJob, "test_create_wd")
                job._create_working_directory()
                self.assertEqual(
                    os.listdir(job.working_directory), ["WARN_pyiron_modified_content"]
                )
            with self.subTest("Suppress writing of warning file"):
                job = self.project.create_job(ToyJob, "test_create_wd2")
                self.project.state.settings.configuration[wd_warn_key] = False
                job._create_working_directory()
                self.assertEqual(os.listdir(job.working_directory), [])
        finally:
            self.project.state.settings.configuration[
                wd_warn_key
            ] = previous_wd_warn_setting

    def test__write_run_wrapper(self):
        pass

    def test__type_to_hdf(self):
        pass

    def test__type_from_hdf(self):
        pass

    def test_childs_to_hdf(self):
        pass

    def test_childs_from_hdf(self):
        pass

    def test_open_hdf(self):
        pass

    def test__check_ham_state(self):
        pass

    def test_error(self):
        ham = self.project.create.job.ScriptJob("job_single_debug")
        self.assertEqual(ham.error.print_queue(), '')
        self.assertEqual(ham.error.print_message(), '')
        self.assertTrue('no error' in ham.error.__repr__())

    def test_compress(self):
        job = self.project.load(self.project.get_job_ids()[0])
        wd_files = job.list_files()
        self.assertEqual(len(wd_files), 1, "Only one zipped file should be present in the working directory")
        self.assertEqual(wd_files[0], f"{job.name}.tar.bz2", "Inconsistent name for the zipped file")

    def test_restart(self):
        job = self.project.load(self.project.get_job_ids()[0])
        job_restart = job.restart()
        job_restart.run()
        wd_files = job_restart.list_files()
        self.assertEqual(len(wd_files), 1, "Only one zipped file should be present in the working directory")
        self.assertEqual(wd_files[0], f"{job_restart.name}.tar.bz2", "Inconsistent name for the zipped file")
        job_restart.decompress()
        wd_files = job_restart.list_files()
        self.assertEqual(
            len(wd_files),
            2,
            "Only one input and the WARN_pyiron_modified_content file should "
            "be present in the working directory",
        )
        self.assertEqual(
            wd_files[0], "input.yml", "Inconsistent name for the zipped file"
        )

    def test_return_codes(self):
        """Jobs exiting with return codes other than job.executable.allowed_codes should be marked as 'aborted'"""

        j = self.project.create_job(ReturnCodeJob, "success_0")
        j.run()
        self.assertTrue(not j.status.aborted, "Job aborted even though return code is 0!")

        j = self.project.create_job(ReturnCodeJob, "aborted_1")
        j.input["return_code"] = 1
        try:
            j.run()
        except RuntimeError:
            pass
        self.assertTrue(j.status.aborted, "Job did not abort even though return code is 1!")

        j = self.project.create_job(ReturnCodeJob, "success_1")
        j.input["accepted_codes"] = [1]
        j.run()
        self.assertTrue(not j.status.aborted, "Job aborted even though return code 1 is explicitely accepted!")

        j = self.project.create_job(ReturnCodeJob, "aborted_2")
        j.input["return_code"] = 2
        j.input["accepted_codes"] = [1]
        try:
            j.run()
        except RuntimeError:
            pass
        self.assertTrue(j.status.aborted, "Job did not abort even though return code is 2!")

if __name__ == "__main__":
    unittest.main()
