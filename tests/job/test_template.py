from pyiron_base._tests import TestWithCleanProject
from pyiron_base.job.template import TemplateJob
from pyiron_base.generic.datacontainer import DataContainer
from pyiron_base.generic.hdfio import ProjectHDFio


class TestTemplateJob(TestWithCleanProject):

    def setUp(self) -> None:
        super().setUp()
        job_name = 'template_job'
        self.job = TemplateJob(
            ProjectHDFio(project=self.project.copy(), file_name=job_name),
            job_name
        )

    def test_io(self):
        self.assertIsInstance(self.job.input, DataContainer)
        self.assertIsInstance(self.job.output, DataContainer)

        with self.assertRaises(AttributeError):
            self.job.input = 'foo'

        with self.assertRaises(AttributeError):
            self.job.output = 'bar'
