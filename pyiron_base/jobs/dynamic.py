import os


def warn_dynamic_job_classes(resource_folder_lst, logger):
    for path in resource_folder_lst:
        if os.path.exists(path):
            sub_folders = os.listdir(path)
            for folder in ["dynamic", "templates"]:
                if folder in sub_folders:
                    logger.warning(
                        "pyiron found a '"
                        + folder
                        + "' folder in the "
                        + path
                        + " resource directory. These are no longer supported in pyiron_base >=0.7.0. "
                        + "They are replaced by Project.create_job_class() and Project.wrap_python_function()."
                    )
