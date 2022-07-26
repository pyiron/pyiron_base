import os


def getdir(path):
    path_base_name = os.path.basename(path)
    if path_base_name == "":
        return os.path.basename(os.path.dirname(path))
    else:
        return path_base_name
