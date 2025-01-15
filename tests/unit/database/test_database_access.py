# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Normally, test-based development is done in reversed order unlike this DatabaseClass.
First the Unittest should be created and then the wanted class which should pass all the tests.
Because of that, this Unittest is based on the DatabaseClass making the executed test cases a bit strange
and specific extra for this class.

Murat Han Celik
"""

import unittest
import os
from datetime import datetime
from random import choice
from string import ascii_uppercase
from typing import List, Optional
from pyiron_base.database.generic import DatabaseAccess
from pyiron_base._tests import PyironTestCase
from sqlalchemy import text

# legacy method of DatabaseAccess; kept here only to test _get_items_dict
def get_items_sql(
    self, where_condition: Optional[str] = None, sql_statement: Optional[str] = None
) -> List[dict]:
    """
    Submit an SQL query to the database

    Args:
        where_condition (str): SQL where query, query like: "project LIKE 'lammps.phonons.Ni_fcc%'"
        sql_statement (str): general SQL query, normal SQL statement

    Returns:
        list: get a list of dictionaries, where each dictionary represents one item of the table like:
             [{u'chemicalformula': u'BO',
              u'computer': u'localhost',
              u'hamilton': u'VAMPS',
              u'hamversion': u'1.1',
              u'id': 1,
              u'job': u'testing',
              u'masterid': None,
              u'parentid': 0,
              u'project': u'database.testing',
              u'projectpath': u'/TESTING',
              u'status': u'KAAAA',
              u'subjob': u'testJob',
              u'timestart': u'2016-05-02 11:31:04.253377',
              u'timestop': u'2016-05-02 11:31:04.371165',
              u'totalcputime': 0.117788,
              u'username': u'User'},
             {u'chemicalformula': u'BO',
              u'computer': u'localhost',
              u'hamilton': u'VAMPS',
              u'hamversion': u'1.1',
              u'id': 2,
              u'job': u'testing',
              u'masterid': 0,
              u'parentid': 0,
              u'project': u'database.testing',
              u'projectpath': u'/TESTING',
              u'status': u'KAAAA',
              u'subjob': u'testJob',
              u'timestart': u'2016-05-02 11:31:04.253377',
              u'timestop': u'2016-05-02 11:31:04.371165',
              u'totalcputime': 0.117788,
              u'username': u'User'}.....]
    """

    if where_condition:
        where_condition = (
            where_condition.replace("like", "similar to")
            if self._engine.dialect.name == "postgresql"
            else where_condition
        )
        try:
            query = "select * from " + self.table_name + " where " + where_condition
            query.replace("%", "%%")
            result = self.conn.execute(text(query))
        except Exception as except_msg:
            print("EXCEPTION in get_items_sql: ", except_msg)
            raise ValueError("EXCEPTION in get_items_sql: ", except_msg)
    elif sql_statement:
        sql_statement = (
            sql_statement.replace("like", "similar to")
            if self._engine.dialect.name == "postgresql"
            else sql_statement
        )
        # TODO: make it save against SQL injection
        result = self.conn.execute(text(sql_statement))
    else:
        result = self.conn.execute(text("select * from " + self.table_name))
    row = result.mappings().all()
    if not self._keep_connection:
        self.conn.close()

    # change the date of str datatype back into datetime object
    output_list = []
    for col in row:
        # ensures working with db entries, which are camel case
        timestop_index = [item.lower() for item in col.keys()].index("timestop")
        timestart_index = [item.lower() for item in col.keys()].index("timestart")
        tmp_values = list(col.values())
        if (tmp_values[timestop_index] and tmp_values[timestart_index]) is not None:
            # changes values
            try:
                tmp_values[timestop_index] = datetime.strptime(
                    str(tmp_values[timestop_index]), "%Y-%m-%d %H:%M:%S.%f"
                )
                tmp_values[timestart_index] = datetime.strptime(
                    str(tmp_values[timestart_index]), "%Y-%m-%d %H:%M:%S.%f"
                )
            except ValueError:
                print("error in: ", str(col))
        output_list += [dict(zip(col.keys(), tmp_values))]
    return output_list

class TestDatabaseAccess(PyironTestCase):
    """
    Standard Unittest of the DatabaseAccess class
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up whole class for testing
        Returns:
        """
        # we assume everything working on sqlite, should also work on postgres in sqlalchemy
        cls.database = DatabaseAccess("sqlite:///test_database.db", "simulation")

    @classmethod
    def tearDownClass(cls):
        """
        Tear down whole class after testing
        Returns:
        """
        cls.database.conn.close()
        if os.name != "nt":
            # On windows we get PermissionError: [WinError 32] The process cannot access the
            # file because it is being used by another process: 'test_database.db'
            os.remove("test_database.db")

    def tearDown(self):
        """
        Deletes all entries after every tested function
        Returns:
        """
        self.database.conn.execute(text("delete from simulation"))

    def test_get_table_headings(self):
        """
        Tests get_table_headings
        Returns:
        """
        heading_list = [
            "id",
            "parentid",
            "masterid",
            "projectpath",
            "project",
            "job",
            "subjob",
            "chemicalformula",
            "status",
            "hamilton",
            "hamversion",
            "username",
            "computer",
            "timestart",
            "timestop",
            "totalcputime",
        ]
        # general headings have to be at least a part of get_table_headings
        for item in heading_list:
            self.assertTrue(item in self.database.get_table_headings())

    def test_add_item_dict(self):
        """
        Tests add_item_dict function
        Returns:
        """
        par_dict = self.add_items("BO")
        key = par_dict["id"]
        # list as parameter shall not work
        self.assertRaises(
            Exception, self.database.add_item_dict, [{"chemicalformula": "BO"}]
        )
        self.assertIsInstance(key, int)  # returned value must be int
        # added and got dict must be(almost) the same
        result = self.database.get_item_by_id(key)
        self.assertTrue(par_dict.items() <= result.items())

    def test_item_update(self):
        """
        Tests item_update function
        Returns:
        """
        par_dict = self.add_items("BO")
        key = par_dict["id"]
        # function does only accept a dict, no list
        self.assertRaises(
            Exception, self.database.item_update, [{"job": "testing2"}], key
        )
        try:  # Function works with int, str and list, normally I would test against list, but then our project
            # would not work anymore.
            self.database.item_update({"job": "testing2"}, key)
            self.database.item_update({"job": "testing2"}, [key])
            self.database.item_update({"job": "testing2"}, str(key))
        except TypeError:
            self.fail(
                "Unexpectedly, item_update raises an Error with types of ids which should be usable"
            )

    def test_delete_item(self):
        """
        Tests delete_item function
        Returns:
        """
        par_dict = self.add_items("BO")
        key = par_dict["id"]
        self.database.delete_item(key)
        self.assertRaises(
            Exception, self.database.delete_item, [key]
        )  # call function with unsupported list as argument
        self.assertRaises(
            RuntimeError, self.database.delete_item, 123456789
        )  # remove non existent job id
        # self.assertRaises(Exception, self.database.get_item_by_id, key)  # ensure item does not exist anymore

    def test_get_item_by_id(self):
        """
        Tests get_item_by_id function
        Returns:
        """
        par_dict = self.add_items("BO")
        key = par_dict["id"]
        self.assertRaises(
            Exception, self.database.get_item_by_id, [key]
        )  # given key must be int or str
        # self.assertRaises(Exception, self.database.get_item_by_id,
        #                   str(key + 1))  # must give Error, if id does not exist
        self.assertIsInstance(
            self.database.get_item_by_id(key), dict
        )  # return value has to be a dict
        # added dict must (almost) be same as the got ones
        result = self.database.get_item_by_id(key)
        self.assertTrue(par_dict.items() <= result.items())

    def test_get_items_dict_and(self):
        """
        Tests the 'and' functionality of get_items_dict function
        Returns:
        """
        self.add_items("Blub")
        # tests general and statements
        item_dict = {"hamilton": "VAMPE", "hamversion": "1.1"}
        self.assertEqual(
            self.database.get_items_dict(item_dict),
            get_items_sql(self.database, "hamilton='VAMPE' and hamversion='1.1'"),
        )

    def test_get_items_dict_project(self):
        """
        Tests whether a query for {'project': 'Projecta%'} gives Projecta, Projecta/b/c , but not Projectas
        Returns:
        """
        par_dict = {
            "chemicalformula": "H2",
            "computer": "localhost",
            "hamilton": "VAMPE",
            "hamversion": "1.1",
            "job": "testing",
            "parentid": 0,
            "project": "Projecta/",
            "projectpath": "/TESTING",
            "status": "KAAAA",
            "timestart": datetime(2016, 5, 2, 11, 31, 4, 253377),
            "timestop": datetime(2016, 5, 2, 11, 31, 4, 371165),
            "totalcputime": 0.117788,
            "username": "User",
            "masterid": 0,
            "subjob": "testJob",
        }
        second_dict = dict(par_dict, project="Projecta/b/c/")
        third_dict = dict(par_dict, project="Projectas")
        par_dict["id"] = self.database.add_item_dict(par_dict)
        second_dict["id"] = self.database.add_item_dict(second_dict)
        third_dict["id"] = self.database.add_item_dict(third_dict)
        self.assertEqual(
            [par_dict, second_dict],
            self.database.get_items_dict({"project": "Projecta/%"}),
        )

    def test_get_items_dict_or(self):
        """
        Tests 'or' functionality of get_items_dict function
        Returns:
        """
        self.add_items("Blub")
        self.add_items("Blab")
        # tests an example or statement
        item_dict = {"chemicalformula": ["Blub", "Blab"]}
        # assert that both the sql and non-sql methods give the same result
        sql_db = get_items_sql(self.database,
            "chemicalformula='Blub' or chemicalformula='Blab'"
        )
        dict_db = self.database.get_items_dict(item_dict)
        for item in sql_db:
            self.assertTrue(item in dict_db)

    def test_get_items_dict_like(self):
        """
        Tests 'like' functionality of get_items_dict function
        Returns:
        """
        self.add_items("Blub")
        # tests an example like statement
        item_dict = {"status": "%AA%"}
        # assert that both the sql and non-sql methods give the same result
        sql_db = get_items_sql(self.database, "status like '%AA%'")
        dict_db = self.database.get_items_dict(item_dict)
        for item in sql_db:
            self.assertTrue(item in dict_db)

    def test_get_items_dict_datatype(self):
        """
        Tests datatype error functionality of get_items_dict function
        Returns:
        """
        # ensures right datatype
        item_dict = ["test", "test"]
        self.assertRaises(TypeError, self.database.get_items_dict, item_dict)

    def test_z_add_column(self):
        """
        Tests add_column function
        Name includes a z so that it is run last. Altering a table can lead
        the other tests to fail.
        Returns:
        """
        self.add_items("blub")
        column = "myColumn5"

        if column not in self.database.get_table_headings():
            self.database.add_column(column, "varchar(50)")
        self.assertRaises(
            Exception, self.database.add_column, column
        )  # cannot add column with same name
        self.assertTrue(
            column in self.database.get_table_headings()
        )  # see whether myColumn has been included
        try:
            # list should be usable, but function will just take last element of lists
            second_column = "".join(
                choice(ascii_uppercase) + str(i) for i in range(12)
            )  # random generator for columns
            self.database.add_column([second_column], ["varchar(50)"])
            self.database.add_column([second_column + "2"], "varchar(50)")
        except TypeError:
            self.fail("Unexpectedly add_column cannot take lists as parameter.")
        self.assertRaises(
            Exception, self.database.add_column, ["mycolumn"], 10
        )  # cannot use int as params

    # NOT A TEST #
    def add_items(self, formula):
        """
        Simple generic helper function to add items to DB
        Args:
            formula: string for chemicalformula

        Returns:
        """
        par_dict = {
            "chemicalformula": formula,
            "computer": "localhost",
            "hamilton": "VAMPE",
            "hamversion": "1.1",
            "job": "testing",
            "parentid": 0,
            "project": "database.testing/",
            "projectpath": "/TESTING",
            "status": "KAAAA",
            "timestart": datetime(2016, 5, 2, 11, 31, 4, 253377),
            "timestop": datetime(2016, 5, 2, 11, 31, 4, 371165),
            "totalcputime": 0.117788,
            "username": "User",
            "masterid": 0,
            "subjob": "testJob",
        }
        par_dict["id"] = self.database.add_item_dict(par_dict)
        return par_dict


if __name__ == "__main__":
    unittest.main()
