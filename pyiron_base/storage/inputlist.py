"""
Backwards compatible way of importing the DataContainer.
"""

from typing import Optional, Union

from pyiron_snippets.deprecate import deprecate

from pyiron_base.storage.datacontainer import DataContainer


class InputList(DataContainer):
    """
    A class representing an input list.

    This class is deprecated. Please use DataContainer instead.

    Args:
        init (Union[None, dict, list, tuple], optional): The initial data to populate the input list with. Defaults to None.
        table_name (str, optional): The name of the table associated with the input list. Defaults to None.
    """

    @deprecate("use DataContainer instead", version="0.3.0")
    def __init__(
        self,
        init: Union[None, dict, list, tuple] = None,
        table_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the InputList object.

        Args:
            init (Union[None, dict, list, tuple], optional): The initial data to populate the input list with. Defaults to None.
            table_name (str, optional): The name of the table associated with the input list. Defaults to None.
        """
        super().__init__(init=init, table_name=table_name)
