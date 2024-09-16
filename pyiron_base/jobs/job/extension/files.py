import os
from itertools import islice
from typing import Generator, List, Optional, Union

from pyiron_base.jobs.job.util import (
    _working_directory_list_files,
    _working_directory_read_file,
)


class File:
    __slots__ = ("_path",)

    def __init__(self, path: str):
        self._path = path

    def __str__(self) -> str:
        return self._path

    def _read(self, tail: Optional[int] = None) -> List[str]:
        return _working_directory_read_file(
            working_directory=os.path.dirname(str(self)),
            file_name=os.path.basename(str(self)),
            tail=tail,
        )

    def __iter__(self) -> Generator:
        return iter(self._read())

    def abspath(self) -> str:
        """
        Absolute path to file object

        Returns:
            str: absolute path
        """
        return os.path.abspath(self._path)

    def list(self, lines: Optional[int] = None) -> List[str]:
        """
        Return file content as list of lines.

        Args:
            lines (int): only return the first `lines` lines

        Return:
            list of str: file content
        """
        return list(islice(iter(self), lines))

    def tail(self, lines: int = 100) -> None:
        """
        Print the last `lines` to stdout.

        Args:
            lines (int): number of output lines
        """
        print(*self._read(tail=lines), sep="")

    def __eq__(self, other: Union[str, "File"]) -> bool:
        return self.__str__().__eq__(other)


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

    >>> job.files.INCAR # doctest: +SKIP
    File('INCAR')
    """

    __slots__ = ("_working_directory",)

    def __init__(self, working_directory: str):
        self._working_directory = os.path.abspath(os.path.expanduser(working_directory))

    def _get_file_dict(self) -> dict:
        return {
            f.replace(".", "_"): f
            for f in _working_directory_list_files(
                working_directory=self._working_directory
            )
        }

    def __dir__(self) -> List[str]:
        return list(self._get_file_dict().keys()) + super().__dir__()

    def list(self) -> List[str]:
        """
        List all files in the working directory of the job.
        """
        return _working_directory_list_files(working_directory=self._working_directory)

    def _ipython_display_(self) -> None:
        path = self._job.working_directory + ":"
        files = [
            "\t" + str(f)
            for f in _working_directory_list_files(
                working_directory=self._working_directory
            )
        ]
        print(os.linesep.join([path, *files]))

    def tail(self, file: str, lines: int = 100) -> None:
        """
        Print the last lines of a file.

        Args:
            file (str): filename
            lines (int): number of lines to print

        Raises:
            FileNotFoundError: if the given file does not exist
        """
        return self[file].tail(lines=lines)

    def __getitem__(self, item: str) -> Union[File, "FileBrowser"]:
        sub = os.path.join(self._working_directory, item)
        if os.path.isdir(sub):
            return FileBrowser(sub)
        if item in _working_directory_list_files(
            working_directory=self._working_directory,
            include_archive=False,
        ):
            return File(os.path.join(self._working_directory, item))
        elif item in _working_directory_list_files(
            working_directory=self._working_directory,
            include_archive=True,
        ):
            return File(os.path.join(self._working_directory, item))
        else:
            raise FileNotFoundError(item)

    def __getattr__(self, item: str) -> Union[File, "FileBrowser"]:
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        else:
            try:
                return self[self._get_file_dict()[item]]
            except KeyError:
                raise FileNotFoundError(item) from None
