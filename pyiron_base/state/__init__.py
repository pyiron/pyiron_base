from pyiron_base.database.manager import database as _database
from pyiron_base.generic.util import Singleton
from pyiron_base.state.logger import logger as _logger
from pyiron_base.state.publications import publications as _publications
from pyiron_base.state.queue_adapter import queue_adapters as _queue_adapters
from pyiron_base.state.settings import settings as _settings

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Oct 22, 2021"


class State(metaclass=Singleton):
    """
    A helper class to give quick and easy access to all the singleton classes which together define the IDE.

    Attributes:
        logger: Self-explanatory.
        publications: Bibliography of papers which should be cited based on the code that was used (alpha release).
        settings: System settings.
        database: Database (or file base) connection.
        queue_adapter: Configuration for using remote resources.
    """
    @property
    def logger(self):
        return _logger

    @property
    def publications(self):
        return _publications

    @property
    def settings(self):
        return _settings

    @property
    def database(self):
        return _database

    @property
    def queue_adapter(self):
        return _queue_adapters.adapter


state = State()
