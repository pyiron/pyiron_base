import sys
from pyiron_base import Project, __version__
pr = Project("tests/static/backwards/")
for job in pr.iter_jobs(recursive = True, convert_to_object = False):
    if job.name == "scriptjob":
        job = job.to_object()
        if job.input.test_argument != 42:
            raise ValueError(
                    "Loading from version {} doesn't restore input.".format(
                        job.project.path
            ))
