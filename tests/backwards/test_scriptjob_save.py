from pyiron_base import Project, __version__
pr = Project(
            "tests/static/backwards/V{}".format(__version__).replace(".", "_")
)
job = pr.create_job(pr.job_type.ScriptJob, "scriptjob")
# scriptjob requires an existing path, so just set this path, since we don't
# intend on running our selves anyway
job.script_path = __file__
job.input['test_argument'] = 42
job.save()
