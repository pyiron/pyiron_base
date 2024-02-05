import os
import subprocess
from conda.core.envs_manager import list_all_known_prefixes


class CondaEnvironment:
    def __dir__(self):
        return list(self._list_all_known_prefixes_dict().keys()) + ["create"]

    @staticmethod
    def _list_all_known_prefixes_dict():
        return {os.path.basename(path): path for path in list_all_known_prefixes()}

    def __getattr__(self, item):
        item_dict = {os.path.basename(path): path for path in list_all_known_prefixes()}
        if item in item_dict.keys():
            return item_dict[item]
        else:
            raise AttributeError(f"Unknown conda environment {item}. Use one of {self._list_all_known_prefixes_dict()} or create a new one.")

    @staticmethod
    def create(env_name, env_file):
        subprocess.check_output(
            ["mamba", "env", "create", "-n", env_name, "-f", env_file, "-y"],
            universal_newlines=True,
        )
