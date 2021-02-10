.. _faq:

===
FAQ
===

How to cite pyiron?
===================

To cite pyiron and the corresponding codes, please follow the instructions on the `publication page <https://pyiron.readthedocs.io/en/latest/source/citation.html>`_.


How to install pyiron?
======================

pyiron is designed to be installed as centralized service on your local computer cluster, rather than a local
installation on each individual workstation. To test pyiron online or with a local installation, please follow the
instructions on the `installation page <installation.html>`_.

How to link your own executable?
================================

The linking of executables is explained as part of the installation `here <installation.html#custom-executables-and-parameter-files>`_. By
default pyiron links to the executables provided by conda but you can accelerate you calculation by compiling your own
version of a given simulation code which is optimized for your hardware.

How to send a calculation to the background?
============================================

While most examples execute calculations inline or in modal mode, it is also possible to send calculation in the
background. 

.. code-block::

  job.server.run_mode.non_modal = True
  job.run()
  print("execute other commands while the job is running.")
  pr.wait_for_job(job)

In this example the job is executed in the background, while the print command is already executed. Afterwards the
project object waits for the execution of the job in the background to be finished.

How to submit a calculation to the queuing system?
==================================================

Just like executing calculation in the background it is also possible to submit calculation to the queuing system:

.. code-block::

  job.server.list_queues()  # returns a list of queues available on the system
  job.server.view_queues()  # returns a DataFrame listing queues and their settings 
  job.server.queue = "my_queue"  # select a queue 
  job.server.cores = 80          # set the number of cores 
  job.server.run_time = 3600     # set the run time in seconds
  job.run()

For the queuing system to be available in pyiron it is necessary to configure it. The configuration of different queuing
systems is explained in the installation.

What is the meaning of the name - pyiron?
=========================================
pyiron is the combination of **py** + **iron** connecting Python, the programming language with iron as pyiron was
initially developed at the Max Planck Institut für Eisenforschung (iron research).

.. toctree::
   :maxdepth:2
