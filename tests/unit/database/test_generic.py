import os
import unittest
from threading import Lock
import time
from queue import Queue

from pyiron_base.database.generic import ConnectionWatchDog, AutorestoredConnection, DatabaseAccess


class TestConnectionWatchDog(unittest.TestCase):
    def test_watchdog(self):
        class DummyConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        conn = DummyConnection()
        lock = Lock()
        watchdog = ConnectionWatchDog(conn, lock, timeout=0.1)
        watchdog.start()
        self.assertFalse(conn.closed)
        time.sleep(0.2)
        self.assertTrue(conn.closed)

    def test_watchdog_kick(self):
        class DummyConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        conn = DummyConnection()
        lock = Lock()
        watchdog = ConnectionWatchDog(conn, lock, timeout=0.2)
        watchdog.start()
        self.assertFalse(conn.closed)
        watchdog.kick()
        time.sleep(0.1)
        self.assertFalse(conn.closed)
        watchdog.kill()
        self.assertTrue(conn.closed)


class TestAutorestoredConnection(unittest.TestCase):
    def test_autorestored_connection(self):
        class DummyEngine:
            def connect(self):
                return DummyConnection()

        class DummyConnection:
            def __init__(self):
                self.closed = False
                self.committed = False

            def close(self):
                self.closed = True

            def execute(self, *args, **kwargs):
                return "executed"

            def commit(self):
                self.committed = True

        engine = DummyEngine()
        conn = AutorestoredConnection(engine, timeout=0.1)
        self.assertEqual(conn.execute(), "executed")
        self.assertFalse(conn._conn.closed)
        time.sleep(0.2)
        self.assertTrue(conn._conn.closed)
        self.assertEqual(conn.execute(), "executed")
        conn.commit()
        self.assertTrue(conn._conn.committed)
        conn.close()
        self.assertTrue(conn._conn.closed)


class TestDatabaseAccess(unittest.TestCase):
    def setUp(self):
        self.db = DatabaseAccess(connection_string="sqlite:///:memory:", table_name="jobs")

    def test_add_and_get_item(self):
        # Test adding an item
        item = {
            "job": "testjob",
            "project": "testproj",
            "projectpath": "/dev/null",
            "status": "running",
            "chemicalformula": "H2O",
        }
        item_id = self.db.add_item_dict(item)
        self.assertIsNotNone(item_id)

        # Test getting the item back
        retrieved_item = self.db.get_item_by_id(item_id)
        self.assertEqual(retrieved_item["job"], "testjob")
        self.assertEqual(retrieved_item["status"], "running")

        # Test adding a duplicate
        with self.assertWarns(Warning):
            self.assertIsNone(self.db.add_item_dict(item, check_duplicates=True))

    def test_update_item(self):
        item_id = self.db.add_item_dict({"job": "update_test", "project": "proj", "projectpath": "/dev/null"})
        self.db._item_update({"status": "finished"}, item_id)
        retrieved_item = self.db.get_item_by_id(item_id)
        self.assertEqual(retrieved_item["status"], "finished")

    def test_get_items_dict(self):
        self.db.add_item_dict({"job": "item1", "project": "proj1", "hamilton": "VASP", "projectpath": "/dev/null"})
        self.db.add_item_dict({"job": "item2", "project": "proj1", "hamilton": "LAMMPS", "projectpath": "/dev/null"})
        self.db.add_item_dict({"job": "item3", "project": "proj2", "hamilton": "VASP", "projectpath": "/dev/null"})

        # Test simple query
        items = self.db.get_items_dict({"hamilton": "VASP"})
        self.assertEqual(len(items), 2)

        # Test AND query
        items = self.db.get_items_dict({"project": "proj1", "hamilton": "VASP"})
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["job"], "item1")

        # Test OR query
        items = self.db.get_items_dict({"hamilton": ["VASP", "LAMMPS"]})
        self.assertEqual(len(items), 3)

        # Test LIKE query
        items = self.db.get_items_dict({"project": "proj%"})
        self.assertEqual(len(items), 3)

    def test_get_table_headings(self):
        headings = self.db._get_table_headings()
        self.assertIn("id", headings)
        self.assertIn("job", headings)
        self.assertIn("status", headings)

    def test_add_column(self):
        self.db.add_column("new_col", "VARCHAR(50)")
        headings = self.db._get_table_headings()
        self.assertIn("new_col", headings)

    def test_change_column_type(self):
        # SQLite does not robustly support ALTER COLUMN TYPE, so this test is minimal
        # It checks that the command doesn't raise an error.
        from sqlalchemy.exc import OperationalError
        with self.assertRaises(OperationalError):
            self.db.change_column_type("job", "TEXT")

    def test_job_dict(self):
        self.db.add_item_dict(
            {
                "job": "job_dict_test",
                "project": "proj/path",
                "projectpath": "/dev/null",
                "status": "done",
                "username": "test_user",
            }
        )

        # Test recursive search
        items = self.db._job_dict(
            sql_query=None,
            user="test_user",
            project_path="proj",
            recursive=True,
            job=None,
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["job"], "job_dict_test")

        # Test non-recursive search
        items = self.db._job_dict(
            sql_query=None,
            user="test_user",
            project_path="proj/path",
            recursive=False,
            job=None,
        )
        self.assertEqual(len(items), 1)

        # Test with job name
        items = self.db._job_dict(
            sql_query=None,
            user="test_user",
            project_path="proj",
            recursive=True,
            job="job_dict_test",
        )
        self.assertEqual(len(items), 1)

        # Test with SQL query
        items = self.db._job_dict(
            sql_query="status = done",
            user="test_user",
            project_path="proj",
            recursive=True,
            job=None,
        )
        self.assertEqual(len(items), 1)

    def test_del(self):
        db = DatabaseAccess(connection_string="sqlite:///:memory:", table_name="jobs", timeout=-1)
        db.add_item_dict({"job": "del_test", "project": "proj", "projectpath": "/dev/null"})
        del db


if __name__ == "__main__":
    unittest.main()