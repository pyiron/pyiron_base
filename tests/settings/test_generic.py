# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.settings.generic import Settings, convert_path
import os
from pathlib import Path
from configparser import ConfigParser, NoOptionError


class TestSettings(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.env = os.environ
        cls.cwd = os.getcwd()
        cls.parser = ConfigParser(inline_comment_prefixes=(";",))
        cls.s = Settings()

    def test_config_update_order(self):
        # Config file in env
        local_loc = Path(self.cwd + "/.pyiron")
        local_val = "settings_test_local"
        local_loc.write_text(f"[DEFAULT]\nTYPE = {local_val}\n")
        self.env["PYIRONCONFIG"] = str(local_loc)

        # Default config file
        default_loc = Path("~/.pyiron").expanduser()
        backup_loc = Path("~/.pyiron_test_config_update_order_backup").expanduser()
        try:
            default_loc.rename(backup_loc)
        except FileNotFoundError:
            pass
        default_val = "settings_test_default"
        default_loc.write_text(f"[DEFAULT]\nTYPE = {default_val}\n")

        # System environment variables
        env_val = "settings_test_env"
        self.env["PYIRONSQLTYPE"] = str(env_val)

        # Now peel them off one at a time to make sure we have the promised order of resolution
        self.s._update_configuration(self.s._configuration)
        self.assertEqual(
            local_val, self.s._configuration["sql_type"],
            msg="A config file specified in the env takes top priority."
        )

        self.env.pop("PYIRONCONFIG")
        local_loc.unlink()
        self.s._update_configuration(self.s._configuration)
        self.assertEqual(
            default_val, self.s._configuration["sql_type"],
            msg="The default config file takes priority over the env variables"
        )

        default_loc.unlink()
        self.s._update_configuration(self.s._configuration)
        self.assertEqual(
            env_val, self.s._configuration["sql_type"],
            msg="Value should be read from system environment"
        )

        # self.env.pop("PYIRONSQLTYPE")
        # self.s._update_configuration(self.s._configuration)
        # self.assertEqual(
        #     s._default_configuration["sql_type"], s._configuration["sql_type"],
        #     msg="Code base default should be used after all other options are exhausted"
        # )
        # TODO: Include the default value in the update loop
        #       Right now it either retains the old value (even if you del s and re-instantiate)
        #       or doesn't know about the field at all (if you s._configuration.pop("sql_type")
        #       But right now I'm not trying to change behaviour, just refactor.

        try:  # Restore any preexisting config file
            backup_loc.rename(default_loc)
        except FileNotFoundError:
            pass

    def test_singleness(self):
        s2 = Settings()
        self.assertIs(self.s, s2, msg="There should only ever be one Settings instance")

    def _pop_conda_env_variables(self):
        conda_keys = ["CONDA_PREFIX", "CONDA_DIR"]
        for conda_key in conda_keys:
            try:
                self.env.pop(conda_key)
            except KeyError:
                pass

    def test_appending_conda_resources(self):
        self._pop_conda_env_variables()
        self.s._update_configuration(self.s._configuration)  # Clean out any old conda paths
        before = len(self.s._configuration["resource_paths"])

        here = Path(".").resolve()
        share = Path("./share").resolve()
        pyiron = Path("./share/pyiron").resolve()
        pyiron.mkdir(parents=True)

        self.env["CONDA_PREFIX"] = str(here)  # Contains /share/pyiron -- should get added
        self.env["CONDA_DIR"] = str(pyiron)  # Does not contain /share/pyiron -- shouldn't get added

        self.s._update_configuration(self.s._configuration)
        print("Searched string", str(pyiron))
        print("Actual resources", self.s._configuration["resource_paths"])
        self.assertTrue(
            str(pyiron) in self.s._configuration["resource_paths"],
            msg="The new resource should have been added"
        )
        self.assertEqual(
            before + 1,
            len(self.s._configuration["resource_paths"]),
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
        local = Path(self.cwd + "/.pyiron")
        p1 = '~/here/is/a/path/'
        p2 = 'here\\is\\another'  # TODO: Is this really good enough? Is it really windowsy enough?
        local.write_text(f"[DEFAULT]\nRESOURCE_PATHS = {p1}, {p2}\n")
        self.env["PYIRONCONFIG"] = str(local)
        self._pop_conda_env_variables()
        self.s._update_configuration(self.s._configuration)
        self.assertListEqual(
            [self._niceify_path(p1), self._niceify_path(p2)],
            self.s._configuration['resource_paths']
        )
        local.unlink()
