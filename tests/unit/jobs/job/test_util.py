# coding: utf-8
"""Unit tests for pyiron_base/jobs/job/util.py"""

import os
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from pyiron_base.jobs.job.util import (
    _get_safe_job_name,
    _is_valid_job_name,
    _get_restart_copy_dict,
    _copy_restart_files,
    _job_compressed_name,
    _job_is_compressed,
    _job_decompress,
    _working_directory_list_files,
    _working_directory_read_file,
    _working_directory_is_compressed,
    _get_compressed_job_name,
    _job_list_files,
    _job_read_file,
    _kill_child,
    _copy_to_delete_existing,
)
from pyiron_base.database.sqlcolumnlength import JOB_STR_LENGTH


class TestIsValidJobName(unittest.TestCase):
    def test_valid_name(self):
        _is_valid_job_name("valid_name_123")  # Should not raise

    def test_invalid_identifier(self):
        with self.assertRaises(ValueError):
            _is_valid_job_name("123invalid")

    def test_too_long_name(self):
        with self.assertRaises(ValueError):
            _is_valid_job_name("a" * (JOB_STR_LENGTH + 1))

    def test_name_with_special_chars(self):
        with self.assertRaises(ValueError):
            _is_valid_job_name("invalid-name")


class TestGetSafeJobName(unittest.TestCase):
    def test_simple_string(self):
        result = _get_safe_job_name("my_job")
        self.assertEqual(result, "my_job")

    def test_special_symbols_replaced(self):
        result = _get_safe_job_name("my.job")
        self.assertEqual(result, "mydjob")  # '.' -> 'd'... wait let me check
        # '.' -> 'd', so 'my.job' -> 'mydjobd'? No: 'my' + 'd' + 'job' = 'mydjobd'
        self.assertNotIn(".", result)

    def test_tuple_input(self):
        result = _get_safe_job_name(("job", 1.5))
        self.assertIn("job", result)
        self.assertIn("1", result)

    def test_float_rounding(self):
        result = _get_safe_job_name(("job", 1.123456789))
        self.assertIn("1d12345679", result)

    def test_no_rounding_when_ndigits_none(self):
        result = _get_safe_job_name(("job", 1.123456789), ndigits=None)
        self.assertIn("1d123456789", result)

    def test_custom_special_symbols(self):
        result = _get_safe_job_name("my_job", special_symbols={})
        self.assertEqual(result, "my_job")

    def test_dash_replaced(self):
        result = _get_safe_job_name("my-job")
        self.assertEqual(result, "mymjob")  # '-' -> 'm'

    def test_space_replaced(self):
        result = _get_safe_job_name("my job")
        self.assertEqual(result, "my_job")  # ' ' -> '_'


class TestGetRestartCopyDict(unittest.TestCase):
    def setUp(self):
        self.job = MagicMock()

    def test_with_dict_mapping(self):
        self.job.restart_file_list = ["/path/to/file1.txt"]
        self.job.restart_file_dict = {"file1.txt": "new_file1.txt"}
        result = _get_restart_copy_dict(self.job)
        self.assertIn("new_file1.txt", result)
        self.assertEqual(result["new_file1.txt"], "/path/to/file1.txt")

    def test_without_dict_mapping(self):
        self.job.restart_file_list = ["/path/to/file2.txt"]
        self.job.restart_file_dict = {}
        result = _get_restart_copy_dict(self.job)
        self.assertIn("file2.txt", result)


class TestCopyRestartFiles(unittest.TestCase):
    def test_raises_if_no_working_dir(self):
        job = MagicMock()
        job.working_directory = "/nonexistent/path"
        job.restart_file_list = ["/some/file.txt"]
        job.restart_file_dict = {}
        with self.assertRaises(ValueError):
            _copy_restart_files(job)

    def test_copies_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src_file = os.path.join(tmpdir, "source.txt")
            work_dir = os.path.join(tmpdir, "workdir")
            os.makedirs(work_dir)
            with open(src_file, "w") as f:
                f.write("test")
            job = MagicMock()
            job.working_directory = work_dir
            job.restart_file_list = [src_file]
            job.restart_file_dict = {}
            _copy_restart_files(job)
            self.assertTrue(os.path.exists(os.path.join(work_dir, "source.txt")))


class TestWorkingDirectoryListFiles(unittest.TestCase):
    def test_empty_for_nonexistent_dir(self):
        result = _working_directory_list_files("/nonexistent/path")
        self.assertEqual(result, [])

    def test_lists_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "file.txt"), "w") as f:
                f.write("test")
            result = _working_directory_list_files(tmpdir)
            self.assertIn("file.txt", result)


class TestWorkingDirectoryIsCompressed(unittest.TestCase):
    def test_uncompressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "file.txt"), "w") as f:
                f.write("test")
            self.assertFalse(_working_directory_is_compressed(tmpdir))

    def test_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            compressed_name = os.path.basename(_get_compressed_job_name(tmpdir))
            # Create the tar file
            tar_path = os.path.join(tmpdir, compressed_name)
            with tarfile.open(tar_path, "w:bz2") as tar:
                pass
            self.assertTrue(_working_directory_is_compressed(tmpdir))


class TestWorkingDirectoryReadFile(unittest.TestCase):
    def test_raises_for_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                _working_directory_read_file(tmpdir, "nonexistent.txt")

    def test_reads_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fname = os.path.join(tmpdir, "test.txt")
            with open(fname, "w") as f:
                f.write("line1\nline2\nline3\n")
            result = _working_directory_read_file(tmpdir, "test.txt")
            self.assertEqual(len(result), 3)

    def test_tail_parameter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fname = os.path.join(tmpdir, "test.txt")
            with open(fname, "w") as f:
                f.write("line1\nline2\nline3\n")
            result = _working_directory_read_file(tmpdir, "test.txt", tail=2)
            self.assertEqual(len(result), 2)

    def test_read_from_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a file, compress it, then remove original
            fname = "data.txt"
            fpath = os.path.join(tmpdir, fname)
            with open(fpath, "w") as f:
                f.write("hello\nworld\n")
            tar_path = _get_compressed_job_name(tmpdir)
            with tarfile.open(tar_path, "w:bz2") as tar:
                tar.add(fpath, arcname=fname)
            os.remove(fpath)
            result = _working_directory_read_file(tmpdir, fname)
            self.assertIn("hello\n", result)

    def test_tail_from_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fname = "data.txt"
            fpath = os.path.join(tmpdir, fname)
            with open(fpath, "w") as f:
                f.write("line1\nline2\nline3\n")
            tar_path = _get_compressed_job_name(tmpdir)
            with tarfile.open(tar_path, "w:bz2") as tar:
                tar.add(fpath, arcname=fname)
            os.remove(fpath)
            result = _working_directory_read_file(tmpdir, fname, tail=1)
            self.assertEqual(len(result), 1)


class TestJobIsCompressed(unittest.TestCase):
    def test_not_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.working_directory = tmpdir
            self.assertFalse(_job_is_compressed(job))

    def test_compressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.working_directory = tmpdir
            tar_path = _job_compressed_name(job)
            with tarfile.open(tar_path, "w:bz2") as tar:
                pass
            self.assertTrue(_job_is_compressed(job))


class TestJobDecompress(unittest.TestCase):
    def test_decompress(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fname = "output.txt"
            fpath = os.path.join(tmpdir, fname)
            with open(fpath, "w") as f:
                f.write("data")
            job = MagicMock()
            job.working_directory = tmpdir
            tar_path = _job_compressed_name(job)
            with tarfile.open(tar_path, "w:bz2") as tar:
                tar.add(fpath, arcname=fname)
            os.remove(fpath)
            _job_decompress(job)
            self.assertTrue(os.path.exists(fpath))
            self.assertFalse(os.path.exists(tar_path))

    def test_decompress_handles_ioerror(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.working_directory = tmpdir
            # No tar file - should not raise
            _job_decompress(job)


class TestJobListFiles(unittest.TestCase):
    def test_list_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = MagicMock()
            job.working_directory = tmpdir
            with open(os.path.join(tmpdir, "file.txt"), "w") as f:
                f.write("test")
            result = _job_list_files(job)
            self.assertIn("file.txt", result)


class TestKillChild(unittest.TestCase):
    def test_non_master_job_does_nothing(self):
        job = MagicMock()
        # static_isinstance will return False for non-GenericMaster
        with patch("pyiron_base.jobs.job.util.static_isinstance", return_value=False):
            _kill_child(job)  # Should not raise

    def test_master_not_running_does_nothing(self):
        job = MagicMock()
        job.status.running = False
        job.status.submitted = False
        with patch("pyiron_base.jobs.job.util.static_isinstance", return_value=True):
            _kill_child(job)  # Should not raise


class TestCopyToDeleteExisting(unittest.TestCase):
    def test_returns_none_when_no_jobs(self):
        project = MagicMock()
        df = MagicMock()
        df.__len__ = MagicMock(return_value=0)
        project.job_table.return_value = df
        result = _copy_to_delete_existing(project, "test_job", False)
        self.assertIsNone(result)

    def test_loads_existing_job_when_delete_false(self):
        project = MagicMock()
        df = MagicMock()
        df.__len__ = MagicMock(return_value=1)
        df.job.values = ["my_job"]
        project.job_table.return_value = df
        project.load.return_value = "loaded_job"
        result = _copy_to_delete_existing(project, "my_job", False)
        self.assertEqual(result, "loaded_job")

    def test_deletes_existing_job_when_delete_true(self):
        project = MagicMock()
        df = MagicMock()
        df.__len__ = MagicMock(return_value=1)
        df.job.values = ["my_job"]
        project.job_table.return_value = df
        result = _copy_to_delete_existing(project, "my_job", True)
        project.remove_job.assert_called_once_with("my_job")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
