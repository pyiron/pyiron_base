# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.generic.singleton import Singleton
from pysqa import QueueAdapter as PySQAAdpter
import os
from pyiron_base.ide.settings import settings

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
    folder in that path is used to initialize a new object.

    The :attribute:`adapter` property then lets you access the first of these that is found, or None if there were no
    queue configuration files.
    """
    def __init__(self):
        self._adapters = []
        for resource_path in settings.configuration["resource_paths"]:
            if (
                os.path.exists(resource_path)
                and "queues" in os.listdir(resource_path)
                and (
                    "queue.yaml" in os.listdir(os.path.join(resource_path, "queues")) or
                    "clusters.yaml" in os.listdir(os.path.join(resource_path, "queues"))
                )
            ):
                self._adapters.append(PySQAAdpter(directory=os.path.join(resource_path, "queues")))

    @property
    def adapter(self):
        """Previous behaviour was to just give the first adapter you find, so keep doing that..."""
        return None if len(self._adapters) == 0 else self._adapters[0]


queue_adapters = QueueAdapters()