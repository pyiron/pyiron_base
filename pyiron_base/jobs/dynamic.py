import os


def warn_dynamic_job_classes(resource_folder_lst, logger):
    for path in resource_folder_lst:
        sub_folders = os.listdir(path)
        if "dynamic" in sub_folders:
            logger.warning(
                "pyiron found a 'dynamic' folder in the "
                + path
                + " resource directory. These are no longer supported in pyiron_base >=0.7.0."
            )
        elif "templates" in sub_folders:
            logger.warning(
                "pyiron found a 'templates' folder in the "
                + path
                + " resource directory. These are no longer supported in pyiron_base >=0.7.0."
            )
