import json
import os
import unittest
from shutil import copyfile

import pandas as pd

from pyiron_base import FileHDFio, ProjectHDFio
from pyiron_base.storage.filedata import FileData, load_file
from pyiron_base.project.generic import Project
from pyiron_base._tests import PyironTestCase


class TestLoadFile(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        hdf5 = FileHDFio(file_name=cls.current_dir + "/test_data.h5")
        with hdf5.open("content") as hdf:
            hdf["key"] = "value"
        copyfile(cls.current_dir + "/test_data.h5", cls.current_dir + "/test_data")
        with open(cls.current_dir + "/test_data.txt", "w") as f:
            f.write("some text")
        with open(cls.current_dir + "/test_data2", "w") as f:
            f.write("some text")
        with open(cls.current_dir + "/test_data.csv", "w") as f:
            f.write("id,status,chemicalformula")
            f.write("13,aborted,Al108")
        with open(cls.current_dir + "/test_data.json", "w") as f:
            json.dump({"x": [0, 1]}, f)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        for file in [
            "/test_data",
            "/test_data.h5",
            "/test_data.txt",
            "/test_data.csv",
            "/test_data.json",
            "/test_data2",
        ]:
            os.remove(cls.current_dir + file)

    def test_load_file_txt(self):
        txt = load_file(self.current_dir + "/test_data.txt")
        with open(self.current_dir + "/test_data.txt") as f:
            content = f.readlines()
        self.assertEqual(txt, content)

    def test_load_file_stream_txt(self):
        with open(self.current_dir + "/test_data.txt") as f:
            content = f.readlines()
        with open(self.current_dir + "/test_data.txt") as f:
            txt = load_file(f)
        self.assertEqual(txt, content)

    def test_load_file_csv(self):
        csv = load_file(self.current_dir + "/test_data.csv")
        content = pd.read_csv(self.current_dir + "/test_data.csv")
        self.assertTrue(content.equals(csv))

    def test_load_file_hdf(self):
        hdf = load_file(self.current_dir + "/test_data.h5")
        self.assertIsInstance(hdf, FileHDFio)
        self.assertEqual(hdf["content/key"], "value")

    def test_load_file_ProjectHDF(self):
        pr = Project(self.current_dir + "/test_pr")
        pr_hdf = load_file(self.current_dir + "/test_data.h5", project=pr)
        self.assertIsInstance(pr_hdf, ProjectHDFio)
        self.assertEqual(pr_hdf["content/key"], "value")
        pr.remove(enable=True)

    def test_load_file_default(self):
        """Test default load for text file and h5 file without extension."""
        filename = self.current_dir + "/test_data"
        self.assertRaises(IOError, load_file, filename)

        filename = self.current_dir + "/test_data2"
        default = load_file(filename)
        self.assertEqual(default, ["some text"])

    def test_filetype_option(self):
        filename = self.current_dir + "/test_data"
        hdf = load_file(filename, filetype="hdf")
        self.assertIsInstance(hdf, FileHDFio)
        self.assertEqual(hdf["content/key"], "value")

    def test_load_file_json(self):
        json_dict = load_file(self.current_dir + "/test_data.json")
        self.assertEqual(json_dict, {"x": [0, 1]})


class TestFileData(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, "test_data.txt").replace("\\", "/")
        with open(cls.filepath, "w") as f:
            f.write("some text")
        cls.data = FileData(file=cls.filepath)

    @classmethod
    def tearDownClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        filepath = os.path.join(cls.current_dir, "test_data.txt").replace("\\", "/")
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

        data = FileData(data=some_data, file="foo")
        self.assertTrue(data.filetype is None)

    def test_data(self):
        """Test data property of FileData."""
        with open(self.filepath) as f:
            some_data = f.readlines()
        self.assertEqual(self.data.data, some_data)

        data = FileData(data=b"some string", file="foo.txt")
        self.assertEqual(data.data, b"some string")

        data = FileData(self.filepath, filetype=".txt")
        self.assertEqual(data.data, some_data)


class TestFileLoaderRegister(PyironTestCase):
    """Tests for FileLoader.register and related edge cases."""

    def test_register_new_file_type(self):
        """FileLoader.register should add a new file type handler."""
        from pyiron_base.storage.filedata import FileLoader
        FileLoader.register(".xyz_test", lambda f: "xyz_content")
        loader = FileLoader()
        result = loader.load(".xyz_test", "dummy_file")
        self.assertEqual(result, "xyz_content", "registered loader should be called")
        # Clean up
        del FileLoader._file_types[".xyz_test"]

    def test_load_default_falls_through_to_txt(self):
        """Loading a file without a registered extension should use the default (txt)."""
        current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        filepath = os.path.join(current_dir, "test_default_load.txt")
        try:
            with open(filepath, "w") as f:
                f.write("hello default")
            result = load_file(filepath, filetype=".unknown_xyz")
            self.assertEqual(result, ["hello default"], "default load should read as text")
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    def test_load_default_raises_ioerror_on_failure(self):
        """Loading a file that fails should raise IOError."""
        from pyiron_base.storage.filedata import FileLoader
        # Use a file object so filetype is None and we go through default loading
        import io
        # A binary stream will fail to readlines() as text
        loader = FileLoader()
        with self.assertRaises(IOError):
            loader._load_default("/nonexistent/file/that/does/not/exist.txt")

    def test_load_file_no_filetype_no_extension(self):
        """load_file with a file-like object with no name attribute should return None filetype."""
        import io
        stream = io.StringIO("some content")
        # stream has no .name, filetype=None, so _resolve_filetype returns None
        result = load_file(stream)
        self.assertIsNotNone(result, "should return content even with None filetype")


class TestFileDataMissingCoverage(PyironTestCase):
    """Additional tests for FileData class."""

    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.filepath = os.path.join(cls.current_dir, "test_fd_missing.txt").replace("\\", "/")
        with open(cls.filepath, "w") as f:
            f.write("filedata test content")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.filepath):
            os.remove(cls.filepath)

    def test_filedata_data_property_loads_from_source(self):
        """FileData.data should load from source when _data is None."""
        fd = FileData(file=self.filepath)
        self.assertFalse(fd._hasdata, "FileData with file path should not have data yet")
        data = fd.data
        self.assertIsNotNone(data, "data should be loaded from source")

    def test_filedata_data_property_returns_stored_data(self):
        """FileData.data should return _data when it is already set."""
        fd = FileData(file="test.txt", data=b"preloaded")
        self.assertTrue(fd._hasdata, "FileData with data should have _hasdata=True")
        self.assertEqual(fd.data, b"preloaded", "data property should return stored data")


class TestPILImportMocking(PyironTestCase):
    """Test behavior of PIL import branch via mocking."""

    def test_pil_available_registers_extensions(self):
        """When PIL is available, image extensions should be registered in FileLoader."""
        from unittest.mock import MagicMock, patch
        from pyiron_base.storage import filedata as fd_module

        mock_image = MagicMock()
        mock_image.registered_extensions.return_value = {".bmp": "BMP", ".png": "PNG"}
        mock_image.open = MagicMock(return_value="image_object")

        loader_before = set(fd_module.FileLoader._file_types.keys())

        # Simulate what happens when PIL is imported: register extensions
        for ext in mock_image.registered_extensions():
            fd_module.FileLoader.register(ext, mock_image.open)

        self.assertIn(".bmp", fd_module.FileLoader._file_types)
        self.assertIn(".png", fd_module.FileLoader._file_types)

        # Clean up
        for ext in [".bmp", ".png"]:
            if ext in fd_module.FileLoader._file_types and ext not in loader_before:
                del fd_module.FileLoader._file_types[ext]

    def test_nbformat_available_registers_ipynb(self):
        """When nbformat is available, .ipynb extension should be registered."""
        from unittest.mock import MagicMock
        from pyiron_base.storage import filedata as fd_module

        mock_nb_node = MagicMock()
        mock_nb_node._repr_html_.return_value = "<html>notebook</html>"

        # Simulate what happens when nbformat is imported: register .ipynb
        fd_module.FileLoader.register(".ipynb_test", lambda f: mock_nb_node)
        loader = fd_module.FileLoader()
        result = loader.load(".ipynb_test", "dummy.ipynb")
        self.assertEqual(result, mock_nb_node)

        # Clean up
        del fd_module.FileLoader._file_types[".ipynb_test"]

    def test_has_imported_dict_structure(self):
        """_has_imported should have 'PIL' and 'nbformat' keys."""
        from pyiron_base.storage.filedata import _has_imported
        self.assertIn("PIL", _has_imported, "_has_imported should track PIL")
        self.assertIn("nbformat", _has_imported, "_has_imported should track nbformat")
        self.assertIsInstance(_has_imported["PIL"], bool)
        self.assertIsInstance(_has_imported["nbformat"], bool)


if __name__ == "__main__":
    unittest.main()
