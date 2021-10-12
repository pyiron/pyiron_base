from pyiron_base import Project, __version__
pr = Project("tests/static/backwards/")
for job in pr.iter_jobs(recursive = True, convert_to_object = False):
    if job.name == "toy_table":
        job = job.to_object()
        df = job.get_dataframe()
        if len(df) != 2:
            raise ValueError(
                "Loading from version {} doesn't load table correctly.".format(
                job.project.path
            ))
