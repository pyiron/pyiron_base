import json
import os
import unittest
from shutil import copyfile

import pandas as pd

from pyiron_base import FileHDFio, ProjectHDFio
from pyiron_base.generic.filedata import FileData, load_file
from pyiron_base.project.generic import Project


class TestLoadFile(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        hdf5 = FileHDFio(file_name=cls.current_dir + "/test_data.h5")
        with hdf5.open("content") as hdf:
            hdf['key'] = "value"
        copyfile(cls.current_dir + "/test_data.h5", cls.current_dir + "/test_data")
        with open(cls.current_dir + '/test_data.txt', 'w') as f:
            f.write("some text")
        with open(cls.current_dir + '/test_data2', 'w') as f:
            f.write("some text")
        with open(cls.current_dir + '/test_data.csv', 'w') as f:
            f.write("id,status,chemicalformula")
            f.write("13,aborted,Al108")
        with open(cls.current_dir + '/test_data.json', 'w') as f:
            json.dump({'x': [0, 1]}, f)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        for file in ['/test_data', "/test_data.h5",  "/test_data.txt",
                     "/test_data.csv", "/test_data.json", "/test_data2"]:
            os.remove(cls.current_dir + file)

    def test_load_file_txt(self):
        txt = load_file(self.current_dir+'/test_data.txt')
        with open(self.current_dir+'/test_data.txt') as f:
            content = f.readlines()
        self.assertEqual(txt, content)

    def test_load_file_stream_txt(self):
        with open(self.current_dir + '/test_data.txt') as f:
            content = f.readlines()
        with open(self.current_dir + '/test_data.txt') as f:
            txt = load_file(f)
        self.assertEqual(txt, content)

    def test_load_file_csv(self):
        csv = load_file(self.current_dir+'/test_data.csv')
        content = pd.read_csv(self.current_dir+'/test_data.csv')
        self.assertTrue(content.equals(csv))

    def test_load_file_hdf(self):
        hdf = load_file(self.current_dir+'/test_data.h5')
        self.assertIsInstance(hdf, FileHDFio)
        self.assertEqual(hdf['content/key'], 'value')

    def test_load_file_ProjectHDF(self):
        pr = Project(self.current_dir + '/test_pr')
        pr_hdf = load_file(self.current_dir + '/test_data.h5', project=pr)
        self.assertIsInstance(pr_hdf, ProjectHDFio)
        self.assertEqual(pr_hdf['content/key'], 'value')
        pr.remove(enable=True)

    def test_load_file_default(self):
        """Test default load for text file and h5 file without extension."""
        filename = self.current_dir + '/test_data'
        self.assertRaises(IOError, load_file, filename)

        filename = self.current_dir + '/test_data2'
        default = load_file(filename)
        self.assertEqual(default, ["some text"])

    def test_filetype_option(self):
        filename = self.current_dir + '/test_data'
        hdf = load_file(filename, filetype="hdf")
        self.assertIsInstance(hdf, FileHDFio)
        self.assertEqual(hdf['content/key'], 'value')

    def test_load_file_json(self):
        json_dict = load_file(self.current_dir+'/test_data.json')
        self.assertEqual(json_dict, {'x': [0, 1]})


class TestFileData(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        with open(cls.filepath, 'w') as f:
            f.write("some text")
        cls.data = FileData(file=cls.filepath)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        os.remove(filepath)

    def test___init__(self):
        """Test init of data class."""
        # Test if init from setUpClass is as expected
        self.assertFalse(self.data._hasdata)
        self.assertEqual(self.data.filename, "test_data.txt")
        self.assertEqual(self.data.filetype, "txt")

        self.assertRaises(TypeError, FileData)
        data = FileData(file=self.filepath, metadata={"some": "dict"})
        self.assertFalse(data._hasdata)
        self.assertEqual(data.filename, "test_data.txt")
        self.assertEqual(data.filetype, "txt")
        self.assertEqual(data.metadata["some"], "dict")

        with open(self.filepath) as f:
            some_data = f.readlines()
        self.assertRaises(TypeError, FileData, data=some_data)
        data = FileData(data=some_data, file="test_data.dat")
        self.assertTrue(data._hasdata)
        self.assertEqual(data.filetype, "dat")

        data = FileData(data=some_data, file="test_data.dat", filetype="txt")
        self.assertEqual(data.filetype, "txt")

        data = FileData(data=some_data, file='foo')
        self.assertTrue(data.filetype is None)

    def test_data(self):
        """Test data property of FileData."""
        with open(self.filepath) as f:
            some_data = f.readlines()
        self.assertEqual(self.data.data, some_data)

        data = FileData(data=b'some string', file='foo.txt')
        self.assertEqual(data.data, b'some string')

        data = FileData(self.filepath, filetype='.txt')
        self.assertEqual(data.data, some_data)


if __name__ == '__main__':
    unittest.main()
