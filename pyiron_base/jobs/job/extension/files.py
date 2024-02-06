import os
from typing import List
from pyiron_base.jobs.job.util import (
    _job_list_files,
    _job_read_file,
)


class FileBrowser:
    """
    Allows to browse the files in a job directory.

    By default this object prints itself as a listing of the job directory and
    the files inside.

    >>> job.files
    /path/to/my/job:
    \tpyiron.log
    \terror.out

    Access to the names of files is provided with :meth:`.list`

    >>> job.files.list()
    ['pyiron.log', 'error.out', 'INCAR']

    Access to the contents of files is provided by indexing into this object,
    which returns a list of lines in the file

    >>> job.files['error.out']
    ["Oh no\n", "Something went wrong!\n"]

    The :meth:`.tail` method prints the last lines of a file to stdout

    >>> job.files.tail('error.out', lines=1)
    Something went wrong!

    For files that have valid python variable names can also be accessed by
    attribute notation

    >>> job.files.INCAR
    ["SYSTEM=pyiron\n", "ENCUT=270\n", ...]
    """

    __slots__ = ("_job",)

    def __init__(self, job):
        self._job = job

    def _get_file_dict(self):
        return {f.replace(".", "_"): f for f in _job_list_files(job=self._job)}

    def __dir__(self):
        return list(self._get_file_dict().keys()) + [
            "list",
            "tail",
            "__dir__",
            "__getitem__",
            "__getattr__",
            "_ipython_display_",
        ]

    def list(self) -> List[str]:
        """
        List all files in the working directory of the job.
        """
        return _job_list_files(job=self._job)

    def _ipython_display_(self):
        path = self._job.working_directory + ":"
        files = ["\t" + f for f in _job_list_files(job=self._job)]
        print(os.linesep.join([path, *files]))

    def tail(self, file: str, lines: int = 100):
        """
        Print the last lines of a file.

        Args:
            file (str): filename
            lines (int): number of lines to print

        Raises:
            FileNotFoundError: if the given file does not exist
        """
        print(*_job_read_file(job=self._job, file_name=file, tail=lines), sep="")

    def __getitem__(self, item):
        if item not in _job_list_files(job=self._job):
            raise KeyError(item)

        return _job_read_file(job=self._job, file_name=item)

    def __getattr__(self, item):
        try:
            return self[self._get_file_dict()[item]]
        except KeyError:
            raise AttributeError(item) from None
