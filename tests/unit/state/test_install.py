# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import shutil
import stat
import tarfile
import tempfile
import unittest
from unittest.mock import MagicMock, patch, call

from pyiron_base.state.install import (
    _download_resources,
    install_dialog,
    install_pyiron,
)
from pyiron_base._tests import PyironTestCase


class TestInstall(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.execution_path = os.path.dirname(os.path.abspath(__file__))

    @classmethod
    def tearDownClass(cls):
        execution_path = os.path.dirname(os.path.abspath(__file__))
        shutil.rmtree(os.path.join(execution_path, "resources"))
        shutil.rmtree(os.path.join(execution_path, "project"))
        os.remove(os.path.join(execution_path, "config"))
        try:
            os.remove(os.path.join(execution_path, "pyiron.log"))
        except FileNotFoundError:
            pass

    def test_install(self):
        install_pyiron(
            config_file_name=os.path.join(self.execution_path, "config"),
            resource_directory=os.path.join(self.execution_path, "resources"),
            project_path=os.path.join(self.execution_path, "project"),
            giturl_for_zip_file=None,
        )

        with open(os.path.join(self.execution_path, "config"), "r") as f:
            content = f.readlines()
        self.assertEqual(content[0], "[DEFAULT]\n")
        self.assertIn("PROJECT_PATHS", content[1])
        self.assertIn("RESOURCE_PATHS", content[2])
        self.assertTrue(os.path.exists(os.path.join(self.execution_path, "project")))
        self.assertTrue(os.path.exists(os.path.join(self.execution_path, "resources")))


class TestDownloadResources(PyironTestCase):
    """Tests for _download_resources (lines 42-66)."""

    def test_downloads_and_extracts(self):
        """Happy-path: urlretrieve is called, tar extracted, copytree called."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource_dir = os.path.join(tmpdir, "new_resources")
            zip_file = "test_resources.tar.gz"
            git_folder = "resources_folder"
            giturl = "https://example.com/resources.tar.gz"

            temp_zip = os.path.join(tempfile.gettempdir(), zip_file)
            temp_extract = os.path.join(tempfile.gettempdir(), git_folder)

            mock_tar = MagicMock()
            mock_tar.__enter__ = MagicMock(return_value=mock_tar)
            mock_tar.__exit__ = MagicMock(return_value=False)

            with (
                patch("pyiron_base.state.install.urllib2.urlretrieve") as mock_retrieve,
                patch("pyiron_base.state.install.tarfile.open", return_value=mock_tar),
                patch("pyiron_base.state.install.safe_extract") as mock_extract,
                patch("pyiron_base.state.install.copytree") as mock_copy,
                patch("pyiron_base.state.install.os.remove") as mock_remove,
                patch("pyiron_base.state.install.rmtree") as mock_rmtree,
                patch("pyiron_base.state.install.os.walk", return_value=[]),
            ):
                _download_resources(
                    zip_file=zip_file,
                    resource_directory=resource_dir,
                    giturl_for_zip_file=giturl,
                    git_folder_name=git_folder,
                )

            mock_retrieve.assert_called_once_with(giturl, temp_zip)
            mock_extract.assert_called_once_with(mock_tar, tempfile.gettempdir())
            mock_copy.assert_called_once_with(temp_extract, resource_dir)
            mock_remove.assert_called_once_with(temp_zip)
            mock_rmtree.assert_called_once_with(temp_extract)

    def test_raises_if_resource_dir_already_exists(self):
        """_download_resources raises ValueError if resource_dir exists and is non-empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource_dir = os.path.join(tmpdir, "existing_dir")
            os.makedirs(resource_dir)
            # Put a file in so it's not empty (empty dirs get removed first)
            open(os.path.join(resource_dir, "dummy.txt"), "w").close()

            with (
                patch("pyiron_base.state.install.urllib2.urlretrieve"),
                self.assertRaises(ValueError),
            ):
                _download_resources(
                    zip_file="x.tar.gz",
                    resource_directory=resource_dir,
                    giturl_for_zip_file="https://example.com/x.tar.gz",
                    git_folder_name="x",
                )

    def test_empty_existing_dir_is_removed_then_extracted(self):
        """An empty resource directory is removed so extraction can proceed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource_dir = os.path.join(tmpdir, "empty_dir")
            os.makedirs(resource_dir)
            # leave it empty so it gets removed

            mock_tar = MagicMock()
            mock_tar.__enter__ = MagicMock(return_value=mock_tar)
            mock_tar.__exit__ = MagicMock(return_value=False)

            with (
                patch("pyiron_base.state.install.urllib2.urlretrieve"),
                patch("pyiron_base.state.install.tarfile.open", return_value=mock_tar),
                patch("pyiron_base.state.install.safe_extract"),
                patch("pyiron_base.state.install.copytree"),
                patch("pyiron_base.state.install.os.remove"),
                patch("pyiron_base.state.install.rmtree"),
                patch("pyiron_base.state.install.os.walk", return_value=[]),
            ):
                _download_resources(
                    zip_file="r.tar.gz",
                    resource_directory=resource_dir,
                    giturl_for_zip_file="https://example.com/r.tar.gz",
                    git_folder_name="r",
                )
            # resource_dir should have been removed
            self.assertFalse(os.path.exists(resource_dir))

    def test_chmod_applied_to_sh_files_on_non_windows(self):
        """Shell scripts get executable bit set on non-Windows systems."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource_dir = os.path.join(tmpdir, "res")
            sh_file = "run.sh"

            mock_tar = MagicMock()
            mock_tar.__enter__ = MagicMock(return_value=mock_tar)
            mock_tar.__exit__ = MagicMock(return_value=False)

            walk_result = [(resource_dir, [], [sh_file])]

            with (
                patch("pyiron_base.state.install.urllib2.urlretrieve"),
                patch("pyiron_base.state.install.tarfile.open", return_value=mock_tar),
                patch("pyiron_base.state.install.safe_extract"),
                patch("pyiron_base.state.install.copytree"),
                patch("pyiron_base.state.install.os.remove"),
                patch("pyiron_base.state.install.rmtree"),
                patch("pyiron_base.state.install.os.walk", return_value=walk_result),
                patch("pyiron_base.state.install.os.name", "posix"),
                patch("pyiron_base.state.install.os.path.exists", return_value=False),
                patch("pyiron_base.state.install.os.stat") as mock_stat,
                patch("pyiron_base.state.install.os.chmod") as mock_chmod,
            ):
                mock_stat.return_value.st_mode = 0o644
                _download_resources(
                    zip_file="r.tar.gz",
                    resource_directory=resource_dir,
                    giturl_for_zip_file="https://example.com/r.tar.gz",
                    git_folder_name="r",
                )
            full_path = os.path.join(resource_dir, sh_file)
            mock_chmod.assert_called_once_with(full_path, 0o644 | stat.S_IEXEC)


class TestInstallDialog(PyironTestCase):
    """Tests for install_dialog (lines 106-135)."""

    def test_already_installed_prints_message(self, capsys=None):
        """If config file exists, print 'already installed'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, ".pyiron")
            open(config_file, "w").close()

            env = {"PYIRONCONFIG": config_file}
            with (
                patch.dict(os.environ, env, clear=False),
                patch("builtins.print") as mock_print,
            ):
                install_dialog(silently=True)
            mock_print.assert_called_once()
            self.assertIn("already installed", mock_print.call_args[0][0])

    def test_silent_yes_installs_pyiron(self):
        """silently=True with no config file calls install_pyiron and prints success."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, ".pyiron_nonexistent")
            env = {"PYIRONCONFIG": config_file}

            with (
                patch.dict(os.environ, env, clear=False),
                patch("pyiron_base.state.install.install_pyiron") as mock_install,
                patch("builtins.print") as mock_print,
            ):
                install_dialog(silently=True)

            mock_install.assert_called_once()
            mock_print.assert_called_once()
            self.assertIn("restart", mock_print.call_args[0][0])

    def test_no_answer_raises(self):
        """Answering 'no' at the prompt raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, ".pyiron_nonexistent")
            env = {"PYIRONCONFIG": config_file}

            with (
                patch.dict(os.environ, env, clear=False),
                patch("builtins.input", return_value="no"),
                self.assertRaises(ValueError),
            ):
                install_dialog(silently=False)

    def test_interactive_yes_installs(self):
        """Interactive input of 'yes' calls install_pyiron."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, ".pyiron_nonexistent")
            env = {"PYIRONCONFIG": config_file}

            with (
                patch.dict(os.environ, env, clear=False),
                patch("builtins.input", return_value="yes"),
                patch("pyiron_base.state.install.install_pyiron") as mock_install,
                patch("builtins.print"),
            ):
                install_dialog(silently=False)

            mock_install.assert_called_once()

    def test_default_config_path_used_when_env_not_set(self):
        """When PYIRONCONFIG is not in env, ~/.pyiron is checked."""
        env_without_pyironconfig = {
            k: v for k, v in os.environ.items() if k != "PYIRONCONFIG"
        }
        home_pyiron = os.path.expanduser("~/.pyiron")
        exists_before = os.path.exists(home_pyiron)

        if exists_before:
            with (
                patch.dict(os.environ, env_without_pyironconfig, clear=True),
                patch("builtins.print") as mock_print,
            ):
                install_dialog(silently=True)
            mock_print.assert_called_once()
        else:
            # Don't actually install; just verify it attempts to
            with (
                patch.dict(os.environ, env_without_pyironconfig, clear=True),
                patch("pyiron_base.state.install.install_pyiron") as mock_install,
                patch("builtins.print"),
            ):
                install_dialog(silently=True)
            mock_install.assert_called_once()


class TestInstallPyironWithGiturl(PyironTestCase):
    """Tests for install_pyiron with giturl_for_zip_file (line 165)."""

    def test_download_called_when_giturl_provided(self):
        """Line 165: _download_resources called when giturl_for_zip_file is not None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "config")
            resource_dir = os.path.join(tmpdir, "resources")
            project_dir = os.path.join(tmpdir, "project")

            with (
                patch("pyiron_base.state.install._download_resources") as mock_dl,
                patch("pyiron_base.state.install._write_config_file"),
            ):
                install_pyiron(
                    config_file_name=config_file,
                    resource_directory=resource_dir,
                    project_path=project_dir,
                    giturl_for_zip_file="https://example.com/resources.tar.gz",
                    git_folder_name="resources",
                )

            mock_dl.assert_called_once_with(
                zip_file="resources.tar.gz",
                resource_directory=resource_dir,
                giturl_for_zip_file="https://example.com/resources.tar.gz",
                git_folder_name="resources",
            )
