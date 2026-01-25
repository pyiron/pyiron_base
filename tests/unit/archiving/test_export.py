import os
import unittest
from unittest.mock import patch, call, MagicMock
from pyiron_base import Project
from pyiron_base.project.archiving.export_archive import export_database, copy_h5_files
import pandas as pd
from filecmp import dircmp
import shutil
import tempfile
from pyiron_base._tests import PyironTestCase, ToyJob


class TestPack(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir = "archive_folder"
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir_comp = cls.arch_dir + "_comp"
        cls.pr = Project("test")
        cls.pr.remove_jobs(recursive=True, silently=True)
        job = cls.pr.create_job(job_type=ToyJob, job_name="toy")
        job.run()
        cls.pr.pack(destination_path=cls.arch_dir, compress=False)
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.pr.remove(enable=True)
        uncompressed_pr = Project(cls.arch_dir)
        uncompressed_pr.remove(enable=True, enforce=True)
        if os.path.exists("export.csv"):
            os.remove("export.csv")

    def test_backward_compatibility(self):
        with self.assertRaises(ValueError):
            self.pr.pack(csv_file_name="my_export.csv")

    def test_exportedCSV(self):
        # in the first test, the csv file from the packing function is read
        # and is compared with the return dataframe from export_database
        directory_to_transfer = os.path.basename(self.pr.path[:-1])
        df_exp = export_database(self.pr.job_table()).dropna(axis=1)
        df_exp["hamversion"] = df_exp["hamversion"].astype(float)
        self.assertEqual(df_exp["job"].unique()[0], "toy")
        self.assertEqual(df_exp["id"].unique()[0], 0)

    def test_HDF5(self):
        # first we check whether the toy.h5 file exists
        # in the exported directory
        h5_file_path = self.arch_dir + "/" + self.pr.name + "/toy.h5"
        self.assertTrue(os.path.exists(h5_file_path))

    def test_compress_undefined_destination(self):
        self.pr.pack(compress=True)
        file_path = self.pr.name + ".tar.gz"
        self.assertTrue(os.path.exists(file_path))
        os.remove(file_path)
        with self.assertRaises(ValueError):
            self.pr.pack(compress=False)
        with self.assertRaises(ValueError):
            self.pr.pack(destination_path=self.pr.path, compress=False)

    def test_compress(self):
        # here we check whether the packing function
        # does the compressibility right
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        file_path = self.arch_dir_comp + ".tar.gz"
        self.assertTrue(os.path.exists(file_path))
        os.remove(file_path)

    def test_content(self):
        # here we test the content of the archive_folder and
        # compare it with the content of the project directory
        path_to_compare = self.arch_dir + "/" + self.pr.name
        compare_obj = dircmp(path_to_compare, self.pr.path)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_export_with_targz_extension(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_path = os.path.abspath(os.path.join(os.curdir, temp_dir))
            tar_arch = self.arch_dir_comp + ".tar.gz"
            self.pr.pack(
                destination_path=os.path.join(tmp_path, tar_arch),
                compress=True,
            )
            desirable_lst = [tar_arch]
            desirable_lst.sort()
            content_tmp = os.listdir(tmp_path)
            content_tmp.sort()
            self.assertListEqual(desirable_lst, content_tmp)

    @patch("os.makedirs")
    @patch("shutil.copy2")
    @patch("os.walk")
    def test_copy_h5_files(self, mock_walk, mock_copy2, mock_makedirs):
        src = "/mock/src"
        dst = "/mock/dst"

        # Mock the os.walk() response
        mock_walk.return_value = [
            (
                os.path.normpath("/mock/src"),
                ("subdir1", "subdir2"),
                ("file1.h5", "file2.txt"),
            ),
            (os.path.normpath("/mock/src/subdir1"), (), ("file3.h5", "file4.txt")),
            (os.path.normpath("/mock/src/subdir2"), (), ("file5.h5", "file6.txt")),
        ]

        # Call the function
        copy_h5_files(src, dst)

        # Verify that os.makedirs is called for the destination directories
        makedirs_calls = [
            os.path.normpath(call[0][0]) for call in mock_makedirs.call_args_list
        ]
        expected_dirs = [
            os.path.normpath("/mock/dst"),
            os.path.normpath("/mock/dst/subdir1"),
            os.path.normpath("/mock/dst/subdir2"),
        ]
        for expected_dir in expected_dirs:
            self.assertIn(expected_dir, makedirs_calls)

        # Verify that shutil.copy2 is called correctly for .h5 files
        copy2_calls = [
            tuple([os.path.normpath(c) for c in call[0]])
            for call in mock_copy2.call_args_list
        ]
        expected_copy2_calls = [
            (
                os.path.normpath("/mock/src/file1.h5"),
                os.path.normpath("/mock/dst/file1.h5"),
            ),
            (
                os.path.normpath("/mock/src/subdir1/file3.h5"),
                os.path.normpath("/mock/dst/subdir1/file3.h5"),
            ),
            (
                os.path.normpath("/mock/src/subdir2/file5.h5"),
                os.path.normpath("/mock/dst/subdir2/file5.h5"),
            ),
        ]
        for expected_call in expected_copy2_calls:
            self.assertIn(expected_call, copy2_calls)

        # Ensure no .txt files were copied
        for call_args in copy2_calls:
            src_file, dst_file = call_args
            self.assertFalse(src_file.endswith(".txt"))


if __name__ == "__main__":
    unittest.main()
