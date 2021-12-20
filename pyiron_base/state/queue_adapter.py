# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
This is a central control point for all the queue adapters, i.e. how we talk to remote computing power.
Before this class was written, we only ever used the first queue adapter we found defined in the resources.
That behaviour is maintained, but now with `QueueAdapters` we can trivially extend to using multiple resources and
multiple adapters.
"""

from pyiron_base.generic.util import Singleton
from pysqa import QueueAdapter as PySQAAdpter
import os
from pyiron_base.state.settings import settings

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


class QueueAdapters(metaclass=Singleton):
    """
    Populates a list of :class:`pysqa.QueueAdapter` objects based on info stored in the resource paths defined in the
    settings.

    For each resource path, if either of `queues/queue.yaml` and `queues/clusters.yaml` exist, then the `queues/`
    folder in that path is used to initialize a new object, with preference given to `queue.yaml`.

    The :attribute:`adapter` property then lets you access the *first of these that was found*, or None if there were no
    queue configuration files.
    """

    def __init__(self):
        self._adapters = None
        self.construct_adapters()

    def construct_adapters(self) -> None:
        """Read through the resources and construct queue adapters for all the queue configuration files found."""
        self._adapters = []
        for resource_path in settings.configuration["resource_paths"]:
            if (
                os.path.exists(resource_path)
                and "queues" in os.listdir(resource_path)
                and (
                    "queue.yaml" in os.listdir(os.path.join(resource_path, "queues"))
                    or "clusters.yaml"
                    in os.listdir(os.path.join(resource_path, "queues"))
                )
            ):
                self._adapters.append(
                    PySQAAdpter(directory=os.path.join(resource_path, "queues"))
                )

    @property
    def adapter(self) -> PySQAAdpter:
        """
        A :class:`pysqa.QueueAdapter` constructed from the first appropriate configuration files found among the
        `queues/` subdirectory among the resource paths defined in the settings.
        """
        return None if len(self._adapters) == 0 else self._adapters[0]

    def update(self) -> None:
        """Constructs new queue adapters based on the current settings configuration."""
        self.construct_adapters()


queue_adapters = QueueAdapters()
