class Wrapper(object):
    def __init__(self, project):
        self._project = project

    def python_function(self, python_function):
        """
        Create a pyiron job object from any python function

        Args:
            python_function (callable): python function to create a job object from

        Returns:
            pyiron_base.jobs.flex.pythonfunctioncontainer.PythonFunctionContainerJob: pyiron job object

        Example:

        >>> def test_function(a, b=8):
        >>>     return a+b
        >>>
        >>> from pyiron_base import Project
        >>> pr = Project("test")
        >>> job = pr.wrap.python_function(test_function)
        >>> job.input["a"] = 4
        >>> job.input["b"] = 5
        >>> job.run()
        >>> job.output

        """
        job = self._project.create.job.PythonFunctionContainerJob(
            job_name=python_function.__name__
        )
        job.python_function = python_function
        return job
