import unittest
import os
import json
import pandas as pd

from pyiron_base.generic.filedata import FileData, load_file, DisplayItem
from pyiron_base import FileHDFio


class TestLoadFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        hdf5 = FileHDFio(file_name=cls.current_dir + '/test_data')
        with hdf5.open("content") as hdf:
            hdf['key'] = "value"
        hdf5 = FileHDFio(file_name=cls.current_dir + "/test_data.h5")
        with hdf5.open("content") as hdf:
            hdf['key'] = "value"
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

    def test_load_file_csv(self):
        csv = load_file(self.current_dir+'/test_data.csv')
        content = pd.read_csv(self.current_dir+'/test_data.csv')
        self.assertTrue(content.equals(csv))

    def test_load_file_hdf(self):
        hdf = load_file(self.current_dir+'/test_data.h5')
        self.assertIsInstance(hdf, FileHDFio)
        self.assertEqual(hdf['content/key'], 'value')

    def test_load_file_default(self):
        """Test default load for text file and h5 file without extension"""
        filename = self.current_dir + '/test_data'
        default = load_file(filename)
        # For some reason on linux handles this different than windows:
        self.assertTrue(filename == default or default[0] == '‰HDF\n')
        filename = self.current_dir + '/test_data2'
        default = load_file(filename)
        self.assertEqual(default, ["some text"])

    def test_load_file_json(self):
        json_dict = load_file(self.current_dir+'/test_data.json')
        self.assertEqual(json_dict, {'x': [0, 1]})


class TestFileData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        with open(cls.filepath, 'w') as f:
            f.write("some text")
        cls.data = FileData(source=cls.filepath)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        os.remove(cls.filepath)

    def test___init__(self):
        """Test init of data class"""
        # Test if init from setUpClass is as expected
        self.assertFalse(self.data._hasdata)
        self.assertEqual(self.data.filename, "test_data.txt")
        self.assertEqual(self.data.filetype, "txt")
        print('everything fine so far!')
        # self.assertRaises(ValueError, FileData())
        try:
            FileData()
        except ValueError:
            pass
        else:
            raise
        data = FileData(source=self.filepath, metadata={"some": "dict"})
        self.assertFalse(data._hasdata)
        self.assertEqual(data.filename, "test_data.txt")
        self.assertEqual(data.filetype, "txt")
        self.assertEqual(data.metadata["some"], "dict")
        with open(self.filepath) as f:
            some_data = f.readlines()
        # self.assertRaises(ValueError, FileData(data=some_data))
        try:
            FileData(data=some_data)
        except ValueError:
            pass
        else:
            raise
        data = FileData(data=some_data, filename="test_data.dat")
        self.assertTrue(data._hasdata)
        self.assertEqual(data.filetype, "dat")
        data = FileData(data=some_data, filename="test_data.dat", filetype="txt")
        self.assertEqual(data.filetype, "txt")

    def test_data(self):
        """Test data property of FileData"""
        with open(self.filepath) as f:
            some_data = f.readlines()
        self.assertEqual(self.data.data, some_data)


class TestDisplayItem(unittest.TestCase):
    """Test non ipywidget features, i.e. load a file or give back the object."""
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        with open(cls.filepath, 'w') as f:
            f.write("some text")
        cls.data = FileData(source=cls.filepath)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, 'test_data.txt').replace("\\", "/")
        os.remove(cls.filepath)

    def test_display(self):
        display_item = DisplayItem(self.filepath)
        self.assertEqual(display_item.display(), ["some text"])
        some_list = ['one', 2, '3', True]
        self.assertTrue(display_item.display(some_list) is some_list)
        hdf = FileHDFio(self.current_dir+'/some_filename.h5')
        self.assertTrue(display_item.display(hdf) is hdf)
        some_string = 'some random string'
        self.assertTrue(display_item.display(some_string) is some_string)
        self.assertEqual(display_item.display(self.filepath), ['some text'])


if __name__ == '__main__':
    unittest.main()
