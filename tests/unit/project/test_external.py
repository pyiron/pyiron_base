# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
import os
from pyiron_base.project.external import dump, load
from pyiron_base._tests import TestWithProject


class TestExternal(TestWithProject):
    def test_dump_and_load(self):
        current_dir = os.getcwd()
        truth_dict = {"a": 1, "b": 2, "c": 3}
        self.scriptjob = self.project.create.job.ScriptJob("script")
        for k, v in truth_dict.items():
            self.scriptjob.input[k] = v
        self.scriptjob.server.run_mode.manual = True
        self.scriptjob.script_path = __file__
        self.scriptjob.run()
        os.chdir(self.scriptjob.working_directory)
        reload_dict = load()
        for k, v in truth_dict.items():
            self.assertEqual(reload_dict[k], v)
        reload_dict["d"] = 4
        dump(output_dict=reload_dict)
        os.chdir(current_dir)
        script_job_reload = self.project.load(self.scriptjob.job_id)
        self.assertEqual(script_job_reload["output"].to_builtin()["d"], 4)
