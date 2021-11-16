# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.state.settings import settings as s
import os
from pathlib import Path
from configparser import ConfigParser
from shutil import rmtree


class TestSettings(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.env = os.environ
        cls.cwd = os.getcwd()
        cls.parser = ConfigParser(inline_comment_prefixes=(";",))
        # Backup the existing configuration
        cls.backup_env = dict(cls.env)
        cls.default_loc = Path("~/.pyiron").expanduser()
        cls.backup_loc = Path("~/.pyiron_test_config_update_order_backup").expanduser()
        try:
            cls.default_loc.rename(cls.backup_loc)
        except FileNotFoundError:
            pass

    @classmethod
    def tearDownClass(cls) -> None:
        # Restore the configuration
        cls.env = cls.backup_env
        try:  # Restore any preexisting config file
            cls.backup_loc.rename(cls.default_loc)
        except FileNotFoundError:
            pass
        s.update()

    def setUp(self) -> None:
        self.default_loc.unlink(missing_ok=True)
        for k, _ in self.env.items():
            if "PYIRON" in k:
                self.env.pop(k)

    def test_validate_sql_configuration_completeness(self):
        s._validate_sql_configuration({
            "sql_type": "MySQL",
            "user": "something",
            "sql_user_key": "something",
            "sql_host": "something",
            "sql_database": "something"
        })
        with self.assertRaises(ValueError):  # All other values missing
            s._validate_sql_configuration({
                "sql_type": "MySQL",
                # "user": "something",
                # "sql_user_key": "something",
                # "sql_host": "something",
                # "sql_database": "something"
            })

        s._validate_sql_configuration({
            "sql_type": "Postgres",
            "user": "something",
            "sql_user_key": "something",
            "sql_host": "something",
            "sql_database": "something"
        })
        with self.assertRaises(ValueError):  # One other value missing
            s._validate_sql_configuration({
                "sql_type": "Postgres",
                "user": "something",
                "sql_user_key": "something",
                # "sql_host": "something",
                "sql_database": "something"
            })

        s._validate_sql_configuration({"sql_type": "SQLalchemy", "sql_connection_string": "something"})
        with self.assertRaises(ValueError):  # Connection string missing
            s._validate_sql_configuration({"sql_type": "SQLalchemy"})

        s._validate_sql_configuration({"sql_type": "SQLite"})

        with self.assertRaises(ValueError):  # SQL file can't be None
            s._validate_sql_configuration({"sql_type": "SQLite", "sql_file": None})

        sql_dir = Path("./foo").resolve()
        sql_file = Path("./foo/thedatabase.db").resolve()
        s._validate_sql_configuration({
            "sql_type": "SQLite",
            "sql_file": sql_file
        })
        self.assertTrue(os.path.isdir(sql_dir), msg="Failed to create host dir for SQL file")
        rmtree(sql_dir)

        with self.assertRaises(ValueError):  # Connection string missing
            s._validate_sql_configuration({"sql_type": "SQLalchemy"})

        s._validate_sql_configuration({"user": "nothing_about_sql_type"})

    def test_validate_viewer_configuration_completeness(self):
        s._validate_viewer_configuration({
            "sql_type": "Postgres",
            "sql_view_table_name": "something",
            "sql_view_user": "something",
            "sql_view_user_key": "something"
        })

        with self.assertRaises(ValueError):
            s._validate_viewer_configuration({
                "sql_type": "Postgres",
                # "sql_view_table_name": "something",
                "sql_view_user": "something",
                "sql_view_user_key": "something"
            })

        with self.assertRaises(ValueError):
            s._validate_viewer_configuration({
                "sql_type": "MySQL",  # Right now it ONLY works for postgres
                "sql_view_table_name": "something",
                "sql_view_user": "something",
                "sql_view_user_key": "something"
            })

        s._validate_sql_configuration({"user": "nothing_about_sql_view"})

        with self.assertRaises(ValueError):
            s._validate_sql_configuration({"sql_type": "not_a_valid_type"})

    def test_get_config_from_environment(self):
        os.environ["PYIRONFOO"] = "foo"
        os.environ["PYIRONSQLFILE"] = "bar"
        os.environ["PYIRONPROJECTPATHS"] = "baz"
        env_dict = s._get_config_from_environment()
        self.assertNotIn(
            "foo", env_dict.values(),
            msg="Just having PYIRON in the key isn't enough, it needs to be a real key"
        )
        ref_dict = {
            "sql_file": "bar",
            "project_paths": "baz"
        }
        for k, v in env_dict.items():
            self.assertEqual(ref_dict[k], v, msg="Valid item failed to read from environment")

    def test_get_config_from_file(self):
        self.default_loc.write_text(
            "[HEADING]\n"
            "FOO = foo\n"
            "SQL_FILE = bar\n"
            "[HEADING2]\n"
            "PROJECT_PATHS = baz\n"
            ";USER = boa\n"
        )
        file_dict = s._get_config_from_file()
        self.assertNotIn(
            "foo", file_dict.values(),
            msg="It needs to be a real key"
        )
        self.assertNotIn(
            "boa", file_dict.values(),
            msg="This was commented out and shouldn't be read"
        )
        ref_dict = {
            "sql_file": "bar",
            "project_paths": "baz"
        }
        for k, v in file_dict.items():
            self.assertEqual(ref_dict[k], v, msg="Valid item failed to read from file")

    def test_update(self):
        # System environment variables
        env_val = 1
        self.env["PYIRONCONNECTIONTIMEOUT"] = str(env_val)

        # Config file in env
        local_loc = Path(self.cwd + "/.pyiron")
        local_val = 2
        local_loc.write_text(f"[DEFAULT]\nCONNECTION_TIMEOUT = {local_val}\n")
        self.env["PYIRONCONFIG"] = str(local_loc)

        # Default config file
        default_loc = self.default_loc
        default_val = 3
        default_loc.write_text(f"[DEFAULT]\nCONNECTION_TIMEOUT = {default_val}\n")

        # Now peel them off one at a time to make sure we have the promised order of resolution
        user_val = 0
        s.update({"connection_timeout": user_val})
        self.assertEqual(
            user_val, s.configuration["connection_timeout"],
            msg="User-specified update values should take top priority"
        )

        s.update()
        self.assertEqual(
            env_val, s.configuration["connection_timeout"],
            msg="System environment values take second priority"
        )

        self.env.pop("PYIRONCONNECTIONTIMEOUT")
        s.update()
        self.assertEqual(
            local_val, s.configuration["connection_timeout"],
            msg="A config file specified in the env takes third priority."
        )

        self.env.pop("PYIRONCONFIG")
        local_loc.unlink()
        s.update()
        self.assertEqual(
            default_val, s.configuration["connection_timeout"],
            msg="The default config file is the last thing to be read"
        )

        default_loc.unlink()
        s.update()
        self.assertEqual(
            s.default_configuration["connection_timeout"], s.configuration["connection_timeout"],
            msg="Code base default should be used after all other options are exhausted"
        )

    def test_adding_conda_path_to_resources(self):
        for conda_key in ["CONDA_PREFIX", "CONDA_DIR"]:
            try:
                self.env.pop(conda_key)
            except KeyError:
                pass
        s.update()
        before = len(s.configuration["resource_paths"])
        s.update()
        self.assertEqual(
            before, len(s.configuration["resource_paths"]),
            msg="No conda dirs exist, resources length should not change"
        )

        roots = []
        stems = []
        for p in ["pref", "dir"]:
            root = Path(f"./{p}").resolve()
            stem = Path(f"./{p}/share/pyiron").resolve()
            stem.mkdir(parents=True)
            roots.append(root)
            stems.append(stem)

        self.env["CONDA_PREFIX"] = str(roots[0])
        self.env["CONDA_DIR"] = str(roots[1])

        s.update()
        self.assertTrue(
            any([stems[0].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="The new resource should have been added"
        )
        self.assertFalse(
            any([stems[1].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="Once CONDA_PREFIX path is found, CONDA_DIR should be ignored"
        )

        stems[0].rmdir()
        s.update()
        self.assertFalse(
            any([stems[0].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="The CONDA_PREFIX no longer contains /share/pyiron and should not be present"
        )
        self.assertTrue(
            any([stems[1].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="CONDA_DIR is still valid and should be found after CONDA_PREFIX fails"
        )

        # Clean up
        for r in roots:
            rmtree(str(r))

    @staticmethod
    def _niceify_path(p: str):
        return (Path(p)
                .expanduser()
                .resolve()
                .absolute()
                .as_posix()
                .replace("\\", "/")
                )

    def test_path_conversion(self):
        p1 = '~/here/is/a/path/'
        p2 = 'here\\is\\another'
        s.update({'resource_paths': f'{p1}, {p2}'})
        self.assertListEqual(
            [self._niceify_path(p1), self._niceify_path(p2)],
            s.configuration['resource_paths']
        )

    def test_convert_to_list_of_paths(self):
        paths = f"foo, bar{os.pathsep}baz/"

        self.assertIsInstance(s._convert_to_list_of_paths(paths), list, msg="Wrong output type")

        for p in s._convert_to_list_of_paths(paths):
            self.assertEqual(self._niceify_path(p), p, msg="Failed to convert element from string input")

        for p in s._convert_to_list_of_paths(paths.replace(',', os.pathsep).split(os.pathsep)):
            self.assertEqual(self._niceify_path(p), p, msg="Failed to convert element from list input")

        for p in s._convert_to_list_of_paths(paths, ensure_ends_with='/'):
            self.assertEqual('/', p[-1], msg="End was not ensured")
