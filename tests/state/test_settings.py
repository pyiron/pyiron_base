# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.state.settings import settings as s
import os
from pathlib import Path
from configparser import ConfigParser


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

    def setUp(self) -> None:
        self.default_loc.unlink(missing_ok=True)
        for k, _ in self.env.items():
            if "PYIRON" in k:
                self.env.pop(k)

    def test_validate_sql_configuration_completeness(self):
        s._validate_sql_configuration_completeness({
            "sql_type": "MySQL",
            "user": "something",
            "sql_user_key": "something",
            "sql_host": "something",
            "sql_database": "something"
        })
        with self.assertRaises(ValueError):  # All other values missing
            s._validate_sql_configuration_completeness({
                "sql_type": "MySQL",
                # "user": "something",
                # "sql_user_key": "something",
                # "sql_host": "something",
                # "sql_database": "something"
            })

        s._validate_sql_configuration_completeness({
            "sql_type": "Postgres",
            "user": "something",
            "sql_user_key": "something",
            "sql_host": "something",
            "sql_database": "something"
        })
        with self.assertRaises(ValueError):  # One other value missing
            s._validate_sql_configuration_completeness({
                "sql_type": "Postgres",
                "user": "something",
                "sql_user_key": "something",
                # "sql_host": "something",
                "sql_database": "something"
            })

        s._validate_sql_configuration_completeness({"sql_type": "SQLalchemy", "sql_connection_string": "something"})
        with self.assertRaises(ValueError):  # Connection string missing
            s._validate_sql_configuration_completeness({"sql_type": "SQLalchemy"})

        s._validate_sql_configuration_completeness({"sql_type": "SQLite"})

        s._validate_sql_configuration_completeness({"user": "nothing_about_sql_type"})

    def test_validate_viewer_configuration_completeness(self):
        s._validate_viewer_configuration_completeness({
            "sql_type": "Postgres",
            "sql_view_table_name": "something",
            "sql_view_user": "something",
            "sql_view_user_key": "something"
        })

        with self.assertRaises(ValueError):
            s._validate_viewer_configuration_completeness({
                "sql_type": "Postgres",
                # "sql_view_table_name": "something",
                "sql_view_user": "something",
                "sql_view_user_key": "something"
            })

        with self.assertRaises(ValueError):
            s._validate_viewer_configuration_completeness({
                "sql_type": "MySQL",  # Right now it ONLY works for postgres
                "sql_view_table_name": "something",
                "sql_view_user": "something",
                "sql_view_user_key": "something"
            })

        s._validate_sql_configuration_completeness({"user": "nothing_about_sql_view"})

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
        )
        file_dict = s._get_config_from_file()
        self.assertNotIn(
            "foo", file_dict.values(),
            msg="It needs to be a real key"
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

    def _pop_conda_env_variables(self):
        conda_keys = ["CONDA_PREFIX", "CONDA_DIR"]
        for conda_key in conda_keys:
            try:
                self.env.pop(conda_key)
            except KeyError:
                pass

    def test_appending_conda_resources(self):
        self._pop_conda_env_variables()
        s.update()  # Clean out any old conda paths
        before = len(s.configuration["resource_paths"])

        here = Path(".").resolve()
        share = Path("./share").resolve()
        pyiron = Path("./share/pyiron").resolve()
        pyiron.mkdir(parents=True)

        self.env["CONDA_PREFIX"] = str(here)  # Contains /share/pyiron -- should get added
        self.env["CONDA_DIR"] = str(pyiron)  # Does not contain /share/pyiron -- shouldn't get added

        s.update()
        self.assertTrue(
            any([pyiron.as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="The new resource should have been added"
        )
        self.assertEqual(
            before + 1,
            len(s.configuration["resource_paths"]),
            msg="The new resource should only have been added once, as the other path didn't have share/pyiron"
        )
        pyiron.rmdir()
        share.rmdir()

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
