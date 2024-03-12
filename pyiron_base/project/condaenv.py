import os
import subprocess
import warnings

from conda.core.envs_manager import list_all_known_prefixes


class CondaEnvironment:
    def __init__(self, env_path):
        self._env_path = env_path

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
            raise AttributeError(
                f"Unknown conda environment {item}. Use one of {self._list_all_known_prefixes_dict()} or create a new one."
            )

    def create(self, env_name, env_file, use_mamba=False):
        exe = "mamba" if use_mamba else "conda"
        env_lst = list_all_known_prefixes()
        env_path = os.path.join(os.path.abspath(self._env_path), env_name)
        if env_name not in env_lst and env_path not in env_lst:
            command_lst = [
                exe,
                "env",
                "create",
                "--prefix",
                env_path,
                "-f",
                env_file,
                "-y",
            ]
            subprocess.check_output(
                command_lst,
                universal_newlines=True,
            )
        else:
            warnings.warn(
                "The conda environment "
                + env_name
                + " already exists in "
                + str(env_path)
                + "."
            )
