# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base import ListMaster
from pyiron_base._tests import TestWithProject, ToyJob


def transfer_output_to_input(job_old, job_new):
    job_new.input.data_in = job_old.output.data_out


class TestListMaster(TestWithProject):
    def test_workflow(self):
        self.master_toy = self.project.create_job(ListMaster, "master_list")
        self.master_toy.append(self.project.create_job(ToyJob, "toy_1"))
        self.master_toy.append(self.project.create_job(ToyJob, "toy_2"))
        self.master_toy.run()
        self.assertEqual(len(self.project.job_table()), 3)
        self.assertEqual(
            self.project.load("toy_1").input.data_in,
            self.project.load("toy_2").input.data_in,
        )
        self.assertEqual(
            self.project.load("toy_1").output.data_out,
            self.project.load("toy_2").output.data_out,
        )


if __name__ == "__main__":
    unittest.main()
