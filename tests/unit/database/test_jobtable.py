# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from sqlalchemy import text

from pyiron_base.database.filetable import FileTable
from pyiron_base.database.generic import DatabaseAccess
from pyiron_base.database.jobtable import (
    get_child_ids,
    get_job_id,
    get_job_status,
    get_job_working_directory,
    set_job_status,
)


class TestJobtable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = DatabaseAccess("sqlite:///test_jobtable.db", "simulation")

    @classmethod
    def tearDownClass(cls):
        cls.database.conn.close()
        if os.name != "nt":
            os.remove("test_jobtable.db")

    def tearDown(self):
        self.database.conn.execute(text("delete from simulation"))

    def add_item(self, job, masterid=None, status="finished", project="db.testing/"):
        par_dict = {
            "chemicalformula": "H2",
            "computer": "localhost",
            "hamilton": "VAMPE",
            "hamversion": "1.1",
            "job": job,
            "parentid": 0,
            "project": project,
            "projectpath": "/TESTING",
            "status": status,
            "timestart": datetime(2016, 5, 2, 11, 31, 4),
            "timestop": datetime(2016, 5, 2, 11, 31, 5),
            "totalcputime": 0.1,
            "username": "User",
            "masterid": masterid,
            "subjob": "/" + job,
        }
        par_dict["id"] = self.database.add_item_dict(par_dict)
        return par_dict

    # --- get_job_id ---

    def test_get_job_id_int_passthrough(self):
        self.assertEqual(get_job_id(self.database, None, None, "db.testing/", 42), 42)

    def test_get_job_id_unique_match(self):
        entry = self.add_item("unique_job", project="db.testing/")
        self.assertEqual(
            get_job_id(self.database, None, None, "db.testing/", "unique_job"),
            entry["id"],
        )

    def test_get_job_id_no_match_returns_none(self):
        self.assertIsNone(
            get_job_id(self.database, None, None, "db.testing/", "does_not_exist")
        )

    def test_get_job_id_duplicate_raises_value_error(self):
        self.add_item("dupe_job", project="db.testing/")
        self.add_item("dupe_job", project="db.testing/")
        with self.assertRaises(ValueError):
            get_job_id(self.database, None, None, "db.testing/", "dupe_job")

    def test_get_job_id_filetable_branch(self):
        filetable = MagicMock(spec=FileTable)
        filetable.get_job_id.return_value = 123
        result = get_job_id(filetable, None, None, "some/path", "some_job")
        self.assertEqual(result, 123)
        filetable.get_job_id.assert_called_once_with(
            job_specifier="some_job", project="some/path"
        )

    # --- get_child_ids ---

    def test_get_child_ids_no_master_returns_empty_list(self):
        self.assertEqual(
            get_child_ids(self.database, None, None, "db.testing/", "missing_master"),
            [],
        )

    def test_get_child_ids_returns_matching_children(self):
        master = self.add_item("master_job", project="db.testing/")
        child_finished = self.add_item(
            "child_finished", masterid=master["id"], status="finished"
        )
        child_aborted = self.add_item(
            "child_aborted", masterid=master["id"], status="aborted"
        )
        result = get_child_ids(self.database, None, None, "db.testing/", "master_job")
        self.assertEqual(
            sorted(result), sorted([child_finished["id"], child_aborted["id"]])
        )

    def test_get_child_ids_with_status_filter(self):
        master = self.add_item("master_job_2", project="db.testing/")
        child_finished = self.add_item(
            "child_finished_2", masterid=master["id"], status="finished"
        )
        self.add_item("child_aborted_2", masterid=master["id"], status="aborted")
        result = get_child_ids(
            self.database,
            None,
            None,
            "db.testing/",
            "master_job_2",
            status="finished",
        )
        self.assertEqual(result, [child_finished["id"]])

    def test_get_child_ids_filetable_branch(self):
        filetable = MagicMock(spec=FileTable)
        filetable.get_child_ids.return_value = [1, 2, 3]
        result = get_child_ids(filetable, None, None, "some/path", "master_job")
        self.assertEqual(result, [1, 2, 3])
        filetable.get_child_ids.assert_called_once_with(
            job_specifier="master_job", project="some/path"
        )

    # --- set_job_status / get_job_status / get_job_working_directory ---

    def test_set_and_get_job_status_by_name(self):
        entry = self.add_item("status_job", status="running", project="db.testing/")
        set_job_status(
            self.database, None, None, "db.testing/", "status_job", "finished"
        )
        self.assertEqual(
            get_job_status(self.database, None, None, "db.testing/", "status_job"),
            "finished",
        )
        self.assertEqual(
            self.database.get_item_by_id(entry["id"])["status"], "finished"
        )

    def test_get_job_working_directory(self):
        entry = self.add_item("workdir_job", project="db.testing/")
        working_directory = get_job_working_directory(
            self.database, None, None, "db.testing/", "workdir_job"
        )
        self.assertEqual(
            working_directory,
            self.database.get_job_working_directory(entry["id"]),
        )
        self.assertTrue(working_directory.endswith("workdir_job_hdf5/workdir_job"))


if __name__ == "__main__":
    unittest.main()
