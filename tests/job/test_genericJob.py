# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.project.generic import Project
from pyiron_base.job.generic import GenericJob


class TestGenericJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_genericjob"))

    @classmethod
    def tearDownClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        project = Project(os.path.join(file_location, "test_genericjob"))
        project.remove(enable=True)

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
        self.assertTrue(job_empty.project_hdf5.is_empty)
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
        ham = self.project.create.job.ScriptJob("job_single_debug")
        self.assertEqual("job_single_debug", ham.job_name)
        self.assertEqual("/job_single_debug", ham.project_hdf5.h5_path)
        self.assertEqual(
            cwd + "/test_genericjob/job_single_debug.h5", ham.project_hdf5.file_name
        )
        ham.job_name = "job_single_move"
        ham.to_hdf()
        self.assertEqual("/job_single_move", ham.project_hdf5.h5_path)
        self.assertEqual(
            cwd + "/test_genericjob/job_single_move.h5", ham.project_hdf5.file_name
        )
        self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
        ham.project_hdf5.remove_file()
        self.assertFalse(os.path.isfile(ham.project_hdf5.file_name))
        ham = self.project.create.job.ScriptJob("job_single_debug_2")
        ham.to_hdf()
        self.assertEqual("job_single_debug_2", ham.job_name)
        self.assertEqual("/job_single_debug_2", ham.project_hdf5.h5_path)
        self.assertEqual(
            cwd + "/test_genericjob/job_single_debug_2.h5", ham.project_hdf5.file_name
        )
        self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
        ham.job_name = "job_single_move_2"
        self.assertEqual("/job_single_move_2", ham.project_hdf5.h5_path)
        self.assertEqual(
            cwd + "/test_genericjob/job_single_move_2.h5", ham.project_hdf5.file_name
        )
        self.assertTrue(os.path.isfile(ham.project_hdf5.file_name))
        ham.project_hdf5.remove_file()
        self.assertFalse(os.path.isfile(cwd + "/test_genericjob/job_single_debug_2.h5"))
        self.assertFalse(os.path.isfile(ham.project_hdf5.file_name))

    def test_move(self):
        pr_a = self.project.open("project_a")
        pr_b = self.project.open("project_b")
        ham = pr_a.create.job.ScriptJob("job_moving_easy")
        self.assertFalse(ham.project_hdf5.file_exists)
        self.assertTrue("test_genericjob/project_a/" in ham.project_hdf5.project_path)
        self.assertFalse(ham.project_hdf5.file_exists)
        ham.move_to(pr_b)
        self.assertTrue("test_genericjob/project_b/" in ham.project_hdf5.project_path)
        ham_2 = pr_a.create.job.ScriptJob("job_moving_diff")
        ham_2.to_hdf()
        self.assertTrue("test_genericjob/project_a/" in ham_2.project_hdf5.project_path)
        ham_2.move_to(pr_b)
        self.assertTrue("test_genericjob/project_b/" in ham_2.project_hdf5.project_path)
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
        pass

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


if __name__ == "__main__":
    unittest.main()
