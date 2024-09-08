import os
import subprocess
import warnings

from conda.core.envs_manager import list_all_known_prefixes


class CondaEnvironment:
    def __init__(self, env_path: str):
        """
        Initialize a CondaEnvironment object.

        Args:
            env_path (str): The path to the conda environment.
        """
        self._env_path = env_path

    def __dir__(self):
        return list(self._list_all_known_prefixes_dict().keys()) + ["create"]

    @staticmethod
    def _list_all_known_prefixes_dict() -> dict:
        """
        Return a dictionary of all known conda environment prefixes.

        Returns:
            dict: A dictionary of conda environment prefixes.
        """
        return {os.path.basename(path): path for path in list_all_known_prefixes()}

    def __getattr__(self, item: str) -> str:
        item_dict = {os.path.basename(path): path for path in list_all_known_prefixes()}
        if item in item_dict.keys():
            return item_dict[item]
        else:
            raise AttributeError(
                f"Unknown conda environment {item}. Use one of {self._list_all_known_prefixes_dict()} or create a new one."
            )

    def create(
        self,
        env_name: str,
        env_file: str,
        use_mamba: bool = False,
        global_installation: bool = True,
    ) -> None:
        """
        Create a new conda environment.

        Args:
            env_name (str): The name of the new environment.
            env_file (str): The path to the environment file.
            use_mamba (bool, optional): Whether to use mamba instead of conda. Defaults to False.
            global_installation (bool, optional): Whether to install the environment globally. Defaults to True.

        Raises:
            subprocess.CalledProcessError: If the environment creation command fails.
        """
        exe = "mamba" if use_mamba else "conda"
        env_lst = list_all_known_prefixes()
        env_path = os.path.join(os.path.abspath(self._env_path), env_name)
        if env_name not in env_lst and env_path not in env_lst:
            command_lst = [
                exe,
                "env",
                "create",
            ]
            if not global_installation:
                os.makedirs(self._env_path, exist_ok=True)
                command_lst += [
                    "--prefix",
                    env_path,
                ]
            command_lst += [
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
