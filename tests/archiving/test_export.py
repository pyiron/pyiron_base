import os
import unittest
from pyiron_base import Project
from pyiron_base.archiving.export_archive import export_database
import pandas as pd
from pandas._testing import assert_frame_equal
from filecmp import dircmp
from pyiron_base import PythonTemplateJob
from shutil import rmtree
from pyiron_base._tests import PyironTestCase


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


class TestPack(PyironTestCase):

    @classmethod
    def setUpClass(cls):
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir = 'archive_folder'
        # this is used to create a folder/a compressed file, are not path
        cls.arch_dir_comp = cls.arch_dir + '_comp'
        cls.pr = Project('test')
        cls.pr.remove_jobs_silently(recursive=True)
        cls.job = cls.pr.create_job(job_type=ToyJob, job_name='toy')
        cls.job.run()
        cls.pr.pack(destination_path=cls.arch_dir, compress=False)
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )

    def test_exportedCSV(self):
        # in the first test, the csv file from the packing function is read
        # and is compared with the return dataframe from export_database
        directory_to_transfer = os.path.basename(self.pr.path[:-1])
        self.pr.pack(destination_path=self.arch_dir, compress=False)
        df_read = pd.read_csv('export.csv')
        df_read.drop(df_read.keys()[0], inplace=True, axis=1)
        # this removes the "None/NaN/empty" cells as well as the unnamed column
        df_read.dropna(inplace=True, axis=1)
        df_read['timestart'] = pd.to_datetime(df_read['timestart'])
        df_read["hamversion"] = float(df_read["hamversion"])
        df_exp = export_database(
            self.pr, directory_to_transfer, 'archive_folder'
        ).dropna(axis=1)
        df_exp["hamversion"] = float(df_exp["hamversion"])
        assert_frame_equal(df_exp, df_read)

    def test_HDF5(self):
        # first we check whether the toy.h5 file exists
        # in the exported directory
        h5_file_path = self.arch_dir + '/' + self.pr.name + "/toy.h5"
        self.assertTrue(os.path.exists(h5_file_path))

    def test_compress(self):
        # here we check whether the packing function
        # does the compressibility right
        self.pr.pack(destination_path=self.arch_dir_comp, compress=True)
        file_path = self.arch_dir_comp + ".tar.gz"
        self.assertTrue(os.path.exists(file_path))

    def test_content(self):
        # here we test the content of the archive_folder and
        # compare it with the content of the project directory
        path_to_compare = self.arch_dir + "/" + self.pr.name
        compare_obj = dircmp(path_to_compare, self.pr.path)
        self.assertEqual(len(compare_obj.diff_files), 0)

    def test_export_with_targz_extension(self):
        os.mkdir(os.path.join(os.curdir, 'tmp'))
        tmp_path = os.path.join(os.curdir, 'tmp')
        tar_arch = self.arch_dir_comp + '.tar.gz'
        self.pr.pack(destination_path=os.path.join(tmp_path, tar_arch),
                     csv_file_name=os.path.join(tmp_path, 'exported.csv'), compress=True)
        desirable_lst = [tar_arch, 'exported.csv']
        desirable_lst.sort()
        content_tmp = os.listdir(tmp_path)
        content_tmp.sort()
        try:
            rmtree(tmp_path)
        except Exception as err_msg:
            print(f"deleting unsuccessful: {err_msg}")
        self.assertListEqual(desirable_lst, content_tmp)


if __name__ == "__main__":
    unittest.main()
