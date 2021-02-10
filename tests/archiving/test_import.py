import os
import unittest
from pyiron_base import Project
from pyiron_base.archiving.import_archive import getdir
from pandas._testing import assert_frame_equal
from filecmp import dircmp
from pyiron_base import PythonTemplateJob


class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        """A toyjob to test export/import functionalities."""
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    # This function is executed
    def run_static(self):
        with self.project_hdf5.open("output/generic") as h5out:
            h5out["energy_tot"] = self.input["input_energy"]
        self.status.finished = True

class TestUnpacking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir = 'archive_folder'
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir_comp = cls.arch_dir+'_comp'
        cls.pr = Project('test')
        cls.pr.remove_jobs_silently(recursive=True)
        cls.job = cls.pr.create_job(job_type=ToyJob, job_name="toy")
        cls.job.run()
        cls.pr.pack(destination_path=cls.arch_dir_comp, compress=True)
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )

    def setUp(self):
        self.imp_pr = Project('imported')
        self.imp_pr.remove_jobs_silently(recursive=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)

    def tearDown(self):
        self.imp_pr.remove_jobs_silently(recursive=True)

    def test_import_csv(self):
        df_original = self.pr.job_table()
        df_import = self.imp_pr.job_table()
        df_import.dropna(inplace=True, axis=1)
        df_original.dropna(inplace=True, axis=1)
        df_import.drop('project', inplace=True, axis=1)
        df_original.drop('project', inplace=True, axis=1)
        df_import.drop('id', inplace=True, axis=1)
        df_original.drop('id', inplace=True, axis=1)
        df_import["hamversion"] = float(df_import["hamversion"])
        df_original["hamversion"] = float(df_original["hamversion"])
        assert_frame_equal(df_original, df_import)

    def test_import_compressed(self):
        path_original = self.pr.path
        path_import = self.imp_pr.path
        path_original = getdir(path_original)
        path_import = getdir(path_import)
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_import_uncompress(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs_silently(recursive=True)
        self.imp_pr.unpack(origin_path=self.arch_dir, compress=False)
        path_original = self.pr.path
        path_import = self.imp_pr.path
        path_original = getdir(path_original)
        path_import = getdir(path_import)
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_import_from_proj(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs_silently(recursive=True)
        aux_proj = Project(self.arch_dir)  # an auxilary project
        aux_proj.open(os.curdir)
        self.imp_pr.unpack(aux_proj, compress=False)
        path_original = self.pr.path
        path_import = self.imp_pr.path
        path_original = getdir(path_original)
        path_import = getdir(path_import)
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_load_job(self):
        """Jobs should be able to load from the imported project."""

        self.imp_pr.remove_jobs_silently(recursive=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)
        try:
            j = self.imp_pr.load(self.job.name)
        except Exception as e:
            self.fail(msg="Loading job fails with {}".format(str(e)))

    def test_check_job_parameters(self):
        """Imported jobs should be equal to their originals in all their parameters."""

        self.imp_pr.remove_jobs_silently(recursive=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)

        j = self.imp_pr.load(self.job.name)
        self.assertEqual(self.job.input["input_energy"], j.input["input_energy"],
                         "Input values not properly copied to imported job.")
        self.assertEqual(self.job["output/energy_tot"], j["output/energy_tot"],
                         "Output values not properly copied to imported job.")

if __name__ == "__main__":
    unittest.main()
