"""
Backwards compatible way of importing the DataContainer.
"""

from .datacontainer import DataContainer
from .util import deprecate

from functools import wraps

class InputList(DataContainer):

    @deprecate("use DataContainer instead", version="0.3.0")
    @wraps(DataContainer.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
