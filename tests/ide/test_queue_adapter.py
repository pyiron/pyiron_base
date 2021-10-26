# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.state.queue_adapter import queue_adapters
from pyiron_base.state.settings import settings
from pysqa import QueueAdapter
import os
import shutil


def _write_queue_config(file_name, queue_name):
    with open(file_name, 'w') as f:
        f.write("queue_type: SLURM\n")
        f.write(f"queue_primary: {queue_name}\n")
        f.write("queues:\n")
        f.write(f"  {queue_name}: {{cores_max: 42, cores_min: 1, run_time_max: 42, script: {queue_name}.sh}}\n")
    with open(os.path.join(os.path.dirname(file_name), f"{queue_name}.sh"), 'w') as f:
        f.write("")


class TestQueueAdapters(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """
        Populate three resource directories: one with nothing, one with a queue.yaml, and one with both queue and
        clusters.yaml. Then set these resources as resource paths.
        """
        here = os.getcwd()
        cls.test_resources = os.path.join(here, 'testqueueadapters')
        resources = [os.path.join(cls.test_resources, f'res{i}') for i in [0, 1, 2]]
        file_stems = [None, "queue", "clusters"]
        for i, res in enumerate(resources):
            queue_dir = os.path.join(res, 'queues')
            os.makedirs(queue_dir)
            for j, stem in enumerate(file_stems[:i+1]):
                if stem is None:
                    continue
                _write_queue_config(file_name=os.path.join(queue_dir, stem + ".yaml"), queue_name=stem)

        cls.resource_paths = list(settings.resource_paths)
        settings.configuration["resource_paths"] = [settings.convert_path(p) for p in resources]

        queue_adapters.construct_adapters()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.test_resources)
        settings.configuration["resource_paths"] = cls.resource_paths

    def test_construction(self):
        self.assertEqual(
            2, len(queue_adapters._adapters),
            msg="The zeroth resource does not have yaml files and should not produce a queue"
        )
        self.assertEqual(1, len(queue_adapters._adapters[0].queue_list), msg="The yaml file only has a single queue")
        self.assertEqual(
            "queue", queue_adapters._adapters[1].queue_list[0],
            msg="The queues.yaml should take precedence over clusters.yaml"
        )

    def test_adapter(self):
        self.assertIsInstance(queue_adapters.adapter, QueueAdapter)
