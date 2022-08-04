"""
Backwards compatible way of importing the DataContainer.
"""

from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.utils.deprecate import deprecate


class InputList(DataContainer):
    @deprecate("use DataContainer instead", version="0.3.0")
    def __init__(self, init=None, table_name=None):
        super().__init__(init=init, table_name=table_name)
