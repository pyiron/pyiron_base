import logging
import os
from typing import List


def warn_dynamic_job_classes(
    resource_folder_lst: List[str], logger: logging.Logger
) -> None:
    """
    Warns about deprecated 'dynamic' and 'templates' folders in the resource directory.

    Args:
        resource_folder_lst (List[str]): List of resource folder paths.
        logger (logging.Logger): Logger object for logging warning messages.

    Returns:
        None
    """
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
