# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, join, abspath
from os import remove
from pyiron_base.project.generic import Project
from ..toy_job_run import ToyJob


class TestProjectData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")

    @classmethod
    def tearDownClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        try:
            remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass

    def setUp(self):
        self.project = Project(self.project_name)

    def tearDown(self):
        self.project.remove(enable=True)

    def test_data(self):
        self.assertRaises(KeyError, self.project.data.read)

        self.project.data.foo = "foo"
        self.project.data.write()
        self.project.data.read()
        self.assertEqual(1, len(self.project.data))

        project2 = Project(self.project_name)
        self.assertEqual(1, len(project2.data))
        self.assertEqual(self.project.data.foo, project2.data.foo)


class TestProjectBrowser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")
        cls.project = Project(cls.project_name)
        job = cls.project.create_job(ToyJob, 'testjob')
        job.run()
        hdf = cls.project.create_hdf(cls.project.path, 'test_hdf.h5')
        hdf['key'] = 'value'
        Project(cls.project.path + 'sub')
        with open(cls.project.path+'text.txt', 'w') as f:
            f.write('some text')

    @classmethod
    def tearDownClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")
        project = Project(cls.project_name)
        project.remove(enable=True)
        try:
            remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass

    def test_init_browser(self):
        self.assertTrue(self.project.browser.project is self.project)
        self.assertEqual(self.project.browser.path, self.project.path)
        self.assertFalse(self.project.browser.show_files)
        self.assertTrue(self.project.browser.hide_path)
        self.assertFalse(self.project.browser.fix_path)
        self.assertTrue(self.project.browser._node_as_dirs)

    def test_copy(self):
        browser = self.project.browser.copy()
        self.assertTrue(browser.project is self.project.browser.project)
        self.assertEqual(browser.path, self.project.browser.path)
        self.assertFalse(browser.box is self.project.browser.box)
        self.assertEqual(browser.fix_path, self.project.browser.fix_path)

    def test_configure(self):
        browser = self.project.browser.copy()

        browser.configure(show_files=True)
        self.assertTrue(browser.show_files)
        self.assertTrue(browser.hide_path)
        self.assertFalse(browser.fix_path)

        browser.configure(fix_path=True)
        self.assertTrue(browser.show_files)
        self.assertTrue(browser.hide_path)
        self.assertTrue(browser.fix_path)

        browser.configure(hide_path=True)
        self.assertTrue(browser.show_files)
        self.assertTrue(browser.hide_path)
        self.assertTrue(browser.fix_path)

        browser.configure(hide_path=False)
        self.assertTrue(browser.show_files)
        self.assertFalse(browser.hide_path)
        self.assertTrue(browser.fix_path)

        browser.configure(show_files=False, fix_path=False, hide_path=True)
        self.assertFalse(self.project.browser.show_files)
        self.assertTrue(self.project.browser.hide_path)
        self.assertFalse(self.project.browser.fix_path)

    def test_files(self):
        browser = self.project.browser.copy()
        self.assertEqual(browser.files, [])
        browser.show_files = True
        self.assertEqual(len(browser.files), 3)
        self.assertTrue('testjob.h5' in browser.files)
        self.assertTrue('test_hdf.h5' in browser.files)
        self.assertTrue('text.txt' in browser.files)

    def test_nodes(self):
        self.assertEqual(self.project.browser.nodes, ['testjob'])

    def test_dirs(self):
        self.assertEqual(self.project.browser.dirs, ['sub'])

    def test__on_click_file(self):
        browser = self.project.browser.copy()
        self.assertEqual(browser._clickedFiles, [])
        browser._on_click_file('text.txt')
        self.assertEqual(browser._clickedFiles, [join(browser.path, 'text.txt')])
        self.assertEqual(browser.data.data, ["some text"])

    def test__update_project(self):
        browser = self.project.browser.copy()
        path = join(browser.path, 'testjob')
        browser._update_project(path)
        self.assertIsInstance(browser.project, ToyJob)
        self.assertFalse(browser._node_as_dirs)


if __name__ == '__main__':
    unittest.main()
