import os
import unittest
from pyiron_base import Project
from pyiron_base.project.archiving.import_archive import getdir, extract_archive
from pandas._testing import assert_frame_equal
from filecmp import dircmp
from shutil import rmtree
from pyiron_base._tests import PyironTestCase, ToyJob


class TestUnpacking(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir = "archive_folder"
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir_comp = cls.arch_dir + "_comp"
        cls.pr = Project("test")
        cls.pr.remove_jobs(recursive=True, silently=True)
        cls.job = cls.pr.create_job(job_type=ToyJob, job_name="toy")
        cls.job.run()
        cls.pr.pack(destination_path=cls.arch_dir_comp, compress=True)
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.imp_pr = Project("imported")

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.pr.remove(enable=True)
        uncompressed_pr = Project(cls.arch_dir)
        uncompressed_pr.remove(enable=True, enforce=True)
        os.remove("export.csv")
        os.remove(cls.arch_dir_comp + ".tar.gz")
        cls.imp_pr.remove(enable=True)

    def setUp(self):
        super().setUp()
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)

    def tearDown(self):
        super().tearDown()
        self.imp_pr.remove_jobs(recursive=True, silently=True)

    def test_import_csv(self):
        df_original = self.pr.job_table()
        df_import = self.imp_pr.job_table()
        df_import.dropna(inplace=True, axis=1)
        df_original.dropna(inplace=True, axis=1)
        df_import.drop("project", inplace=True, axis=1)
        df_original.drop("project", inplace=True, axis=1)
        df_import.drop("id", inplace=True, axis=1)
        df_original.drop("id", inplace=True, axis=1)
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

    def test_unpack_to_nested_project(self):
        pr = self.pr.open("nested")
        pr_imp = pr.open("imported")
        pr_imp.unpack(origin_path=self.arch_dir_comp, compress=True)
        path_original = self.pr.path
        path_import = pr_imp.path
        path_original = getdir(path_original)
        path_import = getdir(path_import)
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)
        pr.remove(enable=True)

    def test_unpack_from_other_dir_uncompress(self):
        cwd = os.getcwd()
        pack_path = os.path.join(cwd, "exported")
        os.makedirs(name=pack_path)
        pack_path_comp = os.path.join(pack_path, self.arch_dir_comp)
        pack_path_csv = os.path.join(pack_path, "export.csv")
        self.pr.pack(
            destination_path=pack_path_comp, csv_file_name=pack_path_csv, compress=False
        )
        pr = self.pr.open("nested")
        pr_imp = pr.open("imported")
        pr_imp.unpack(
            origin_path=pack_path_comp, csv_file_name=pack_path_csv, compress=False
        )
        compare_obj = dircmp(pack_path_comp, pr_imp.path)
        self.assertEqual(len(compare_obj.diff_files), 0)
        try:
            rmtree(pack_path)
        except Exception as err_msg:
            print(f"deleting unsuccessful: {err_msg}")

    def test_import_uncompress(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.imp_pr.unpack(origin_path=self.arch_dir, compress=False)
        path_original = self.pr.path
        path_import = self.imp_pr.path
        path_original = getdir(path_original)
        path_import = getdir(path_import)
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_import_from_proj(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs(recursive=True, silently=True)
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
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)
        try:
            j = self.imp_pr.load(self.job.name)
        except Exception as e:
            self.fail(msg="Loading job fails with {}".format(str(e)))

    def test_check_job_parameters(self):
        """Imported jobs should be equal to their originals in all their parameters."""
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)
        j = self.imp_pr.load(self.job.name)
        self.assertEqual(
            self.job.input["data_in"],
            j.input["data_in"],
            "Input values not properly copied to imported job.",
        )
        self.assertEqual(
            self.job["data_out"],
            j["data_out"],
            "Output values not properly copied to imported job.",
        )

    def test_import_with_targz_extension(self):
        cwd = os.getcwd()
        pack_path = os.path.join(cwd, "exported_withTar")
        os.makedirs(name=pack_path)
        tar_arch = self.arch_dir_comp + ".tar.gz"
        pack_path_comp = os.path.join(pack_path, tar_arch)
        pack_path_csv = os.path.join(pack_path, "export.csv")
        self.pr.pack(
            destination_path=pack_path_comp, csv_file_name=pack_path_csv, compress=True
        )
        pr = self.pr.open("nested2")
        pr_imp = pr.open("imported2")
        pr_imp.unpack(
            origin_path=pack_path_comp, csv_file_name=pack_path_csv, compress=True
        )
        # here the 7 is the length of '.tar.gz' string
        extract_archive(pack_path_comp[:-7])
        compare_obj = dircmp(pack_path_comp[:-7], pr_imp.path)
        self.assertEqual(len(compare_obj.diff_files), 0)
        pr.remove(enable=True)
        try:
            rmtree(pack_path)
        except Exception as err_msg:
            print(f"deleting unsuccessful: {err_msg}")


if __name__ == "__main__":
    unittest.main()
