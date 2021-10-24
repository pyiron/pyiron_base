from pyiron_base.database.manager import database as _database
from pyiron_base.generic.util import Singleton
from pyiron_base.ide.logger import logger as _logger
from pyiron_base.ide.publications import publications as _publications
from pyiron_base.ide.queue_adapter import queue_adapters as _queue_adapters
from pyiron_base.ide.settings import settings as _settings

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


class IDE(metaclass=Singleton):
    """
    A helper class to give quick and easy access to all the singleton classes which together define the IDE.

    Attributes:
        settings: System settings.
        database: Database (or file base) connection.
        logger: Self-explanatory.
        queue_adapter: Configuration for using remote resources.
        publications: Bibliography of papers which should be cited based on the code that was used (alpha release).
    """
    # With python >=3.9 we can just use @classmethod and @property together so these can be safe from being overwritten
    # But with earlier versions the implementation is ugly, so live dangerously
    # https://stackoverflow.com/questions/128573/using-property-on-classmethods
    settings = _settings
    database = _database
    logger = _logger
    queue_adapter = _queue_adapters.adapter
    publications = _publications
