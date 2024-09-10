import os
import tarfile


def is_within_directory(directory: str, target: str) -> bool:
    """
    Check if the target path is within the specified directory.

    Args:
        directory (str): The directory path.
        target (str): The target path.

    Returns:
        bool: True if the target path is within the directory, False otherwise.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)

    prefix = os.path.commonprefix([abs_directory, abs_target])

    return prefix == abs_directory


def safe_extract(
    tar: tarfile.TarFile,
    path: str = ".",
    members: list = None,
    *,
    numeric_owner: bool = False,
) -> None:
    """
    Safely extract the contents of a tar file.

    This function checks if the extracted files are within the specified path to prevent path traversal attacks.

    Args:
        tar (tarfile.TarFile): The tar file object.
        path (str, optional): The path to extract the files to. Defaults to ".".
        members (list, optional): The members to extract. Defaults to None.
        numeric_owner (bool, optional): Whether to use numeric owner for extracted files. Defaults to False.

    Raises:
        Exception: If attempted path traversal is detected in the tar file.
    """
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        if not is_within_directory(path, member_path):
            raise Exception("Attempted Path Traversal in Tar File")

    tar.extractall(path, members, numeric_owner=numeric_owner)
