import os
import unittest
from pyiron_base import Project
import pandas as pd
from filecmp import dircmp
from shutil import rmtree, copytree
import tarfile
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
        if os.path.exists(cls.arch_dir_comp + ".tar.gz"):
            os.remove(cls.arch_dir_comp + ".tar.gz")
        if os.path.exists("export.csv"):
            os.remove("export.csv")
        cls.imp_pr.remove(enable=True)

    def setUp(self):
        super().setUp()
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp + ".tar.gz")

    def tearDown(self):
        super().tearDown()
        self.imp_pr.remove_jobs(recursive=True, silently=True)

    def test_inspect(self):
        df = self.pr.unpack_csv(self.arch_dir_comp + ".tar.gz")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    def test_import_csv(self):
        df_original = self.pr.job_table()
        df_import = self.imp_pr.job_table()
        df_import.dropna(inplace=True, axis=1)
        df_original.dropna(inplace=True, axis=1)
        df_import.drop("project", inplace=True, axis=1)
        df_original.drop("project", inplace=True, axis=1)
        df_import.drop("id", inplace=True, axis=1)
        df_original.drop("id", inplace=True, axis=1)
        df_import["hamversion"] = df_import["hamversion"].astype(float)
        df_original["hamversion"] = df_original["hamversion"].astype(float)
        pd._testing.assert_frame_equal(df_original, df_import)

    def test_import_compressed(self):
        path_original = self.pr.path
        path_import = self.imp_pr.path
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_unpack_to_nested_project(self):
        pr = self.pr.open("nested")
        pr_imp = pr.open("imported")
        pr_imp.unpack(origin_path=self.arch_dir_comp + ".tar.gz")
        path_original = self.pr.path
        path_import = pr_imp.path
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)
        pr.remove(enable=True)

    def test_unpack_from_other_dir_uncompress(self):
        cwd = os.getcwd()
        pack_path = os.path.join(cwd, "exported")
        os.makedirs(name=pack_path, exist_ok=True)
        pack_path_comp = os.path.join(pack_path, self.arch_dir_comp)
        self.pr.pack(destination_path=pack_path_comp, compress=False)
        pr = self.pr.open("nested")
        pr_imp = pr.open("imported")
        pr_imp.unpack(origin_path=pack_path_comp)
        compare_obj = dircmp(pack_path_comp, pr_imp.path)
        self.assertEqual(len(compare_obj.diff_files), 0)
        try:
            rmtree(pack_path)
        except Exception as err_msg:
            print(f"deleting unsuccessful: {err_msg}")
        pr.remove(enable=True)

    def test_import_uncompress(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.imp_pr.unpack(origin_path=self.arch_dir)
        path_original = self.pr.path
        path_import = self.imp_pr.path
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_import_from_proj(self):
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        aux_proj = Project(self.arch_dir)  # an auxilary project
        aux_proj.open(os.curdir)
        self.imp_pr.unpack(aux_proj)
        path_original = self.pr.path
        path_import = self.imp_pr.path
        compare_obj = dircmp(path_original, path_import)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_load_job_all(self):
        """Jobs should be able to load from the imported project."""
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.pr.pack(
            destination_path=self.arch_dir_comp, compress=True, copy_all_files=True
        )
        self.imp_pr.unpack(origin_path=self.arch_dir_comp + ".tar.gz")
        try:
            j = self.imp_pr.load(self.job.name)
        except Exception as e:
            self.fail(msg="Loading job fails with {}".format(str(e)))

    def test_load_job(self):
        """Jobs should be able to load from the imported project."""
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp + ".tar.gz")
        try:
            j = self.imp_pr.load(self.job.name)
        except Exception as e:
            self.fail(msg="Loading job fails with {}".format(str(e)))

    def test_check_job_parameters(self):
        """Imported jobs should be equal to their originals in all their parameters."""
        self.imp_pr.remove_jobs(recursive=True, silently=True)
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        self.imp_pr.unpack(origin_path=self.arch_dir_comp + ".tar.gz")
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
        if os.path.exists(pack_path):
            rmtree(pack_path)
        os.makedirs(name=pack_path)
        tar_arch = self.arch_dir_comp + ".tar.gz"
        pack_path_comp = os.path.join(pack_path, tar_arch)
        self.pr.pack(destination_path=pack_path_comp, compress=True)
        pr = self.pr.open("nested2")
        pr_imp = pr.open("imported2")
        pr_imp.unpack(origin_path=pack_path_comp)
        with tarfile.open(pack_path_comp, "r:gz") as tar:
            tar.extractall(path=pack_path_comp[: -len(".tar.gz")])
        compare_obj = dircmp(pack_path_comp[:-7], pr_imp.path)
        self.assertEqual(len(compare_obj.diff_files), 0)
        pr.remove(enable=True)
        try:
            rmtree(pack_path)
        except Exception as err_msg:
            print(f"deleting unsuccessful: {err_msg}")

    def test_backwards_compatibility(self):
        with self.assertRaises(ValueError):
            self.imp_pr.unpack(origin_path=self.arch_dir_comp, compress=True)
        with self.assertRaises(ValueError):
            self.imp_pr.unpack(origin_path=self.arch_dir_comp, csv_file_name="ahoy.csv")


class TestUnpackingBackwardsCompatibility(PyironTestCase):
    def test_import_old_tar(self):
        copytree(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../../static/pack",
            ),
            os.getcwd(),
            dirs_exist_ok=True,
        )
        pr = Project("old_tar")
        self.assertRaises(FileNotFoundError, pr.unpack_csv, "test_pack.tar.gz")
        pr.unpack(origin_path="test_pack.tar.gz")
        job = pr.load("toy")
        self.assertEqual(job.job_name, "toy")
        self.assertEqual(job.input.data_in, 100)
        self.assertEqual(job.output.data_out, 101)
        pr.remove(enable=True, enforce=True)
        os.remove("test_pack.tar.gz")
        os.remove("export.csv")


if __name__ == "__main__":
    unittest.main()
