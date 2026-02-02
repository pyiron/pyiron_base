import os
from unittest import TestCase
from pyiron_base import state, warn_dynamic_job_classes


class TestDynamicWarning(TestCase):
    def test_logging(self):
        static_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "static")
        )
        with self.assertLogs("pyiron_log", level="DEBUG") as lc:
            for folder in ["dynamic", "templates"]:
                dynamic_dir = os.path.abspath(os.path.join(static_dir, folder))
                warning_message = (
                    "WARNING:pyiron_log:pyiron found a '"
                    + folder
                    + "' folder in the "
                    + str(static_dir)
                    + " resource directory. These are no longer supported in pyiron_base >=0.7.0. "
                    + "They are replaced by Project.create_job_class() and Project.wrap_python_function()."
                )
                os.makedirs(dynamic_dir, exist_ok=True)
                warn_dynamic_job_classes(
                    resource_folder_lst=[static_dir], logger=state.logger
                )
                self.assertTrue(warning_message in lc.output)
                os.rmdir(dynamic_dir)
