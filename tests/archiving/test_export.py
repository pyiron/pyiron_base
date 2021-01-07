import os
import unittest
from pyiron_base import Project
from pyiron_base.archiving.export_archive import export_database
import pandas as pd
from pandas._testing import assert_frame_equal
from filecmp import dircmp
from pyiron_base import PythonTemplateJob

class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        """
        a toyjob to test export/import functionalities
        """
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100
    # This function is executed
    def run_static(self):
        with self.project_hdf5.open("output/generic") as h5out:
            h5out["energy_tot"] = self.input["input_energy"]
        self.status.finished = True


class TestPacking(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.arch_path = 'archive_folder'
        cls.arch_path_comp = cls.arch_path+'_comp'
        cls.pr = Project('test')
        cls.pr.remove_jobs_silently(recursive=True)
        cls.job = cls.pr.create_job(job_type=ToyJob, job_name='toy')
        cls.job.run()
        cls.pr.packing(destination_path=cls.arch_path,compress=False)
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
    def test_exportedCSV(self):
        ## in the first test, the csv file from the packing function is read
        ## and is compared with the return dataframe from export_database function
        self.pr.packing(destination_path=self.arch_path,compress=False)
        df_read = pd.read_csv('export.csv')
        df_read.drop(df_read.keys()[0],inplace=True,axis = 1)
        df_read.dropna(inplace = True, axis=1) ## this remove the "None/NaN/empty" cells as well as the unnamed column
        df_read['timestart'] = pd.to_datetime(df_read['timestart'])
        df_read["hamversion"]= float(df_read["hamversion"])
        df_exp = export_database(self.pr, self.pr.path,'archive_folder').dropna(axis=1)
        df_exp["hamversion"]= float(df_exp["hamversion"])
        assert_frame_equal(df_exp,df_read)
    
    def test_HDF5(self):
        ## first we check whether the toy.h5 file exists in the exported directory
        h5_file_path = self.arch_path+'/'+self.pr.name+"/toy.h5"
        self.assertTrue(os.path.exists(h5_file_path))

    def test_compress(self):
        ## here we check whether the packing function does the compressibility right
        self.pr.packing(destination_path=self.arch_path_comp,compress=True)
        file_path = self.arch_path_comp+".tar.gz"
        self.assertTrue(os.path.exists(file_path))
    
    def test_content(self):
        ## here we test the content of the archive_folder and compare it with the content of the project directory
        path_to_compare = self.arch_path + "/" + self.pr.name
        compare_obj = dircmp(path_to_compare,self.pr.path)
        self.assertEqual(len(compare_obj.diff_files),0)

if __name__ == "__main__":
    unittest.main()