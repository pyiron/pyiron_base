import os


class Files:
    def __init__(self, working_directory):
        self._working_directory = working_directory

    def __dir__(self):
        return list(self._get_file_convert_dict().keys())

    def _get_file_convert_dict(self):
        return {f.replace(".", "_"): f for f in os.listdir(self._working_directory)}

    def __getattr__(self, attr):
        convert_dict = self._get_file_convert_dict()
        if attr in convert_dict.keys():
            return os.path.join(self._working_directory, convert_dict[attr])
        else:
            raise AttributeError()
