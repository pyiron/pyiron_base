import os
import unittest
from pyiron_base import Project
from pyiron_base.archiving.export_archive import export_database
from pyiron_base.archiving import export_archive
from sample_job import ToyJob
import pandas as pd
from pandas._testing import assert_frame_equal
from filecmp import dircmp

class TestPacking(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        #print('set up the class)
        cls.arch_path = 'archive_folder'
        cls.arch_path_comp = cls.arch_path+'_comp'
        cls.pr = Project('test')
        cls.pr.remove_jobs_silently(recursive=True)
        cls.job = cls.pr.create_job(job_type=ToyJob, job_name='toy')
        cls.job.run()
        cls.pr.packing(destination_path=cls.arch_path,compress=False)
    
    def test_exportedCSV(self):
        ## in the first test, the csv file from the packing function is read
        ## and is compared with the return dataframe from export_database function   
        df_read = pd.read('export.csv')
        df_read.drop(df_read.keys()[0],inplace=True,axis = 1).dropna(inplace = True, axis=1) ## this remove the "None/NaN/empty" cells as well as the unnamed column
        df_read['timestart'] = pd.to_datetime(df_read['timestart'])
        df_read["hamversion"]= float(df_read["hamversion"])
        assert_frame_equal(export_database(self.pr, self.pr.path,'archive_folder').dropna(axis=1),df_read)
        ## In the second test, an examplary.csv file is read and compared with the
        ## one produced by the packing function
        df_known = pd.read_csv("exemplary.csv")
        df_read = pd.read('export.csv')
        df_known.dropna(inplace = True, axis= 1).drop(["timestart","computer"],inplace=True,axis=1)
        df_read.dropna(inplace = True, axis= 1).drop(["timestart","computer"],inplace=True,axis=1)
        assert_frame_equal(df_known,df_read) 
    
    def test_HDF5(self):
        ## first we check whether the toy.h5 file exists in the exported directory
        file_path = self.arch_path+"/toy.h5"
        self.assertTrue(os.path.exists(file_path))
        ## second test, check whether the sample_hdf.h5 is the same as toy.h5
        self.assertEqual(os.system("h5diff sample_hdf.h5 archive_folder/toy.h5"),0)

    def test_compress(self):
        ## here we check whether the packing function does the compressibility right
        self.pr.packing(destination_path=self.arch_path_comp,compress=True)
        file_path = self.arch_path_comp+"tar.gz"
        self.assertTrue(os.path.exists(file_path))
    
    def test_content(self):
        ## here we test the content of the archive_folder and compare it with the content of the project directory
        compare_obj = dircmp(self.arch_path,self.pr.path)
        self.assertEqual(len(compare_obj.diff_files),0)

if __name__ == "__main__":
    unittest.main()