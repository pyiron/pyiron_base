# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from copy import copy
from unittest import TestCase
from pyiron_base.state.settings import settings as s, PYIRON_DICT_NAME
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
        super().setUp()
        try:
            self.default_loc.unlink()
        except FileNotFoundError:
            pass
        for k, _ in self.env.items():
            if "PYIRON" in k:
                self.env.pop(k)
        s.update()

    def test_default_works(self):
        s._configuration = {}
        s.update(s.default_configuration)
        self.assertDictEqual(s.configuration, s.default_configuration)

    def test_validate_sql_configuration_completeness(self):
        s._validate_sql_configuration(
            {
                "sql_type": "MySQL",
                "user": "something",
                "sql_user_key": "something",
                "sql_host": "something",
                "sql_database": "something",
            }
        )
        with self.assertRaises(ValueError):  # All other values missing
            s._validate_sql_configuration(
                {
                    "sql_type": "MySQL",
                    # "user": "something",
                    # "sql_user_key": "something",
                    # "sql_host": "something",
                    # "sql_database": "something"
                }
            )

        s._validate_sql_configuration(
            {
                "sql_type": "Postgres",
                "user": "something",
                "sql_user_key": "something",
                "sql_host": "something",
                "sql_database": "something",
            }
        )
        with self.assertRaises(ValueError):  # One other value missing
            s._validate_sql_configuration(
                {
                    "sql_type": "Postgres",
                    "user": "something",
                    "sql_user_key": "something",
                    # "sql_host": "something",
                    "sql_database": "something",
                }
            )

        s._validate_sql_configuration(
            {"sql_type": "SQLalchemy", "sql_connection_string": "something"}
        )
        with self.assertRaises(ValueError):  # Connection string missing
            s._validate_sql_configuration({"sql_type": "SQLalchemy"})

        s._validate_sql_configuration({"sql_type": "SQLite"})

        with self.assertRaises(ValueError):  # SQL file can't be None
            s._validate_sql_configuration({"sql_type": "SQLite", "sql_file": None})

        sql_dir = Path("./foo").resolve()
        sql_file = Path("./foo/thedatabase.db").resolve()
        s._validate_sql_configuration({"sql_type": "SQLite", "sql_file": sql_file})
        self.assertTrue(
            os.path.isdir(sql_dir), msg="Failed to create host dir for SQL file"
        )
        rmtree(sql_dir)

        with self.assertRaises(ValueError):  # Connection string missing
            s._validate_sql_configuration({"sql_type": "SQLalchemy"})

        s._validate_sql_configuration({"user": "nothing_about_sql_type"})

    def test_validate_no_database_configuration(self):
        with self.assertRaises(ValueError):
            s._validate_no_database_configuration(
                {
                    "disable_database": True,
                    "project_check_enabled": True,
                    "project_paths": [],
                }
            )
        with self.assertRaises(ValueError):
            s._validate_no_database_configuration(
                {
                    "disable_database": True,
                    "project_check_enabled": False,
                    "project_paths": ["/my/path/a"],
                }
            )

    def _test_config_and_credential_synchronization(self):
        if s.credentials is not None and PYIRON_DICT_NAME in s.credentials:
            for key in s.credentials[PYIRON_DICT_NAME]:
                self.assertEqual(
                    s.credentials[PYIRON_DICT_NAME][key], s.configuration[key]
                )

    def test_get_config_from_environment(self):
        self.env["PYIRONFOO"] = "foo"
        self.env["PYIRONSQLFILE"] = "bar"
        self.env["PYIRONPROJECTPATHS"] = "baz"
        with self.subTest("no interference"):
            env_dict = s._get_config_from_environment()
            self.assertNotIn(
                "foo",
                env_dict.values(),
                msg="Just having PYIRON in the key isn't enough, it needs to be a real key",
            )
            ref_dict = {"sql_file": "bar", "project_paths": "baz"}
            self.assertDictEqual(
                ref_dict, env_dict, msg="Valid item failed to read from environment"
            )

        self.env["PYIRONCONFIG"] = "."
        with self.subTest(
            "Should use environment variables even if PYIRONCONFIG is specified."
        ):
            env_dict = s._get_config_from_environment()
            self.assertNotIn(
                "foo",
                env_dict.values(),
                msg="Just having PYIRON in the key isn't enough, it needs to be a real key",
            )
            ref_dict = {"sql_file": "bar", "project_paths": "baz"}
            self.assertEqual(
                ref_dict, env_dict, msg="Valid item failed to read from environment"
            )

        local_loc = Path(self.cwd + "/.pyiron_credentials")
        local_loc.write_text(
            "[DEFAULT]\nPASSWD = something_else\n[OTHER]\nNoPyironKey = token"
        )
        local_loc_str = s.convert_path_to_abs_posix(str(local_loc))
        self.env["PYIRONCREDENTIALSFILE"] = local_loc_str
        with self.subTest("Should be aware of credentials file if specified"):
            env_dict = s._get_config_from_environment()
            self.assertNotIn(
                "foo",
                env_dict.values(),
                msg="Just having PYIRON in the key isn't enough, it needs to be a real key",
            )
            ref_dict = {
                "sql_file": "bar",
                "project_paths": "baz",
                "credentials_file": local_loc_str,
            }
            self.assertEqual(ref_dict, env_dict)
        with self.subTest("Credential file should be read at full update"):
            s._update_from_dict(env_dict)
            self.assertEqual(s.configuration["sql_user_key"], "something_else")
        local_loc.unlink()

    def test_standard_credentials(self):
        self.assertEqual(
            s.credentials,
            {PYIRON_DICT_NAME: {"sql_user_key": None}},
        )

    def test_update_from_env_with_credential_check(self):
        self.env["PYIRONSQLFILE"] = "bar"
        self.env["PYIRONPROJECTPATHS"] = "baz"
        local_loc = Path(self.cwd + "/.pyiron_credentials")
        local_loc.write_text(
            f"[{PYIRON_DICT_NAME}]\nPASSWD = something_else\n[OTHER]\nNoPyironKey = token"
        )
        local_loc_str = s.convert_path_to_abs_posix(str(local_loc))
        self.env["PYIRONCREDENTIALSFILE"] = local_loc_str
        env_dict = s._get_config_from_environment()
        ref_dict = {
            "sql_file": "bar",
            "project_paths": "baz",
            "credentials_file": local_loc_str,
        }
        self.assertEqual(ref_dict, env_dict)
        with self.subTest("credentials dict to update the config"):
            new_env_dict = s._get_credentials_from_file(env_dict)
            ref_dict["sql_user_key"] = "something_else"
            self.assertEqual(new_env_dict, ref_dict)
            self.assertEqual(s.configuration, s.default_configuration)

        with self.subTest("Check for updated config from credentials file"):
            s._update_from_dict(
                env_dict
            )  # updating only the configuration from credential file!
            self.test_standard_credentials()

        with self.subTest("Full population of credentials"):
            credentials_dict = s._add_credentials_from_file()
            cred_ref_dict = {
                "OTHER": {"nopyironkey": "token"},
                PYIRON_DICT_NAME: {"sql_user_key": "something_else"},
            }
            self.assertEqual(credentials_dict, cred_ref_dict)
            self.test_standard_credentials()

        with self.subTest("full update"):
            s.update(env_dict)
            self.assertEqual(s.credentials, cred_ref_dict)

        with self.subTest("credentials and config should be in sync"):
            self._test_config_and_credential_synchronization()
        local_loc.unlink()

    def test__parse_config_file(self):
        local_loc = Path(self.cwd + "/.pyiron_credentials")
        local_loc.write_text(
            f"[DEFAULT]\nPASSWD = something_else\nNoValidKey = None\n[OTHER]\nKey = Value"
        )
        config = s._parse_config_file(
            local_loc, map_dict={"PASSWD": "sql_user_key", "KEY": "key"}
        )
        ref_dict = {"sql_user_key": "something_else", "key": "Value"}
        self.assertDictEqual(ref_dict, config)
        local_loc.unlink()

    def test__add_credentials_from_file(self):
        local_loc = Path(self.cwd + "/.pyiron_credentials")
        local_loc.write_text(
            f"[{PYIRON_DICT_NAME}]\nPASSWD = something_else\nNoValidKey = None\n[OTHER]\nKey = Value\n[SOME]\nN=W"
        )
        local_loc_str = s.convert_path_to_abs_posix(str(local_loc))
        bak = copy(s._configuration)
        s._configuration["credentials_file"] = local_loc_str

        config = s._add_credentials_from_file()
        s._configuration = bak
        ref_dict = {
            PYIRON_DICT_NAME: {"sql_user_key": "something_else", "novalidkey": "None"},
            "OTHER": {"key": "Value"},
            "SOME": {"n": "W"},
        }
        self.assertDictEqual(ref_dict, config)
        local_loc.unlink()

    def test_get_config_from_file(self):
        self.default_loc.write_text(
            "[HEADING]\n"
            "FOO = foo\n"
            "FILE = bar\n"
            "[HEADING2]\n"
            "PROJECT_PATHS = baz\n"
            ";USER = boa\n"
        )

        file_dict = s._get_config_from_file()

        self.assertNotIn("foo", file_dict.values(), msg="It needs to be a real key")
        self.assertNotIn(
            "boa",
            file_dict.values(),
            msg="This was commented out and shouldn't be read",
        )
        ref_dict = {"sql_file": "bar", "project_paths": "baz"}
        self.assertDictEqual(
            ref_dict, file_dict, msg="Valid item failed to read from file"
        )

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
            user_val,
            s.configuration["connection_timeout"],
            msg="User-specified update values should take top priority",
        )

        s.update()
        self.assertEqual(
            env_val,
            s.configuration["connection_timeout"],
            msg="System environment values take second priority",
        )

        self.env.pop("PYIRONCONNECTIONTIMEOUT")
        s.update()
        self.assertEqual(
            local_val,
            s.configuration["connection_timeout"],
            msg="A config file specified in the env takes third priority.",
        )

        self.env.pop("PYIRONCONFIG")
        local_loc.unlink()
        s.update()
        self.assertEqual(
            default_val,
            s.configuration["connection_timeout"],
            msg="The default config file is the last thing to be read",
        )

        default_loc.unlink()
        s.update()
        self.assertEqual(
            s.default_configuration["connection_timeout"],
            s.configuration["connection_timeout"],
            msg="Code base default should be used after all other options are exhausted",
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
            before,
            len(s.configuration["resource_paths"]),
            msg="No conda dirs exist, resources length should not change",
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
            msg="The new resource should have been added",
        )
        self.assertFalse(
            any([stems[1].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="Once CONDA_PREFIX path is found, CONDA_DIR should be ignored",
        )

        stems[0].rmdir()
        s.update()
        self.assertFalse(
            any([stems[0].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="The CONDA_PREFIX no longer contains /share/pyiron and should not be present",
        )
        self.assertTrue(
            any([stems[1].as_posix() in p for p in s.configuration["resource_paths"]]),
            msg="CONDA_DIR is still valid and should be found after CONDA_PREFIX fails",
        )

        # Clean up
        for r in roots:
            rmtree(str(r))

    @staticmethod
    def _niceify_path(p: str):
        return Path(p).expanduser().resolve().absolute().as_posix().replace("\\", "/")

    def test_path_conversion(self):
        p1 = "~/here/is/a/path/"
        p2 = "here\\is\\another"
        s.update({"resource_paths": f"{p1}, {p2}"})
        self.assertListEqual(
            [self._niceify_path(p1), self._niceify_path(p2)],
            s.configuration["resource_paths"],
        )

    def test_convert_to_list_of_paths(self):
        paths = f"foo, bar{os.pathsep}baz/"

        self.assertIsInstance(
            s._convert_to_list_of_paths(paths), list, msg="Wrong output type"
        )

        for p in s._convert_to_list_of_paths(paths):
            self.assertEqual(
                self._niceify_path(p),
                p,
                msg="Failed to convert element from string input",
            )

        for p in s._convert_to_list_of_paths(
            paths.replace(",", os.pathsep).split(os.pathsep)
        ):
            self.assertEqual(
                self._niceify_path(p),
                p,
                msg="Failed to convert element from list input",
            )

        for p in s._convert_to_list_of_paths(paths, ensure_ends_with="/"):
            self.assertEqual("/", p[-1], msg="End was not ensured")
