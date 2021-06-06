
from abc import ABC, abstractmethod

class WithHDF:
    __slots__ = ("_hdf", "_group_name")

    def __init__(self, hdf, group_name=None):
        self._hdf = hdf
        self._group_name = group_name

    def __enter__(self):
        if self._group_name is not None:
            self._hdf = self._hdf.open(self._group_name)

        return self._hdf

    def __exit__(self, *args):
        if self._group_name is not None:
            self._hdf.close()

class HasHDF(ABC):

    __version__ = "0.1.0"
    __hdf_version__ = "0.1.0"

    @abstractmethod
    def _from_hdf(self, hdf, version=None):
        pass

    @abstractmethod
    def _to_hdf(self, hdf):
        pass

    @abstractmethod
    def _get_hdf_group_name(self):
        pass

    def _type_to_hdf(self, hdf):
        hdf["NAME"] = self.__class__.__name__
        hdf["TYPE"] = str(type(self))
        hdf["VERSION"] = self.__version__
        hdf["HDF_VERSION"] = self.__hdf_version__

    def from_hdf(self, hdf, group_name=None):
        group_name = group_name or self._get_group_name()
        with WithHDF(hdf, group_name) as hdf:
            version = hdf.get("HDF_VERSION", "0.1.0")
            self._from_hdf(hdf, version=version)

    def to_hdf(self, hdf, group_name=None):
        with WithHDF(hdf, group_name) as hdf:
            self._type_to_hdf(hdf)
            self._to_hdf(hdf)

    def rewrite_hdf(self, hdf, group_name=None):
        with WithHDF(hdf, group_name) as hdf:
            obj = self.__class__()
            obj.from_hdf(hdf)
            hdf.remove(...)
            obj.to_hdf(hdf)
