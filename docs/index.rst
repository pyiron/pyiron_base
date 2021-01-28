.. pyiron documentation master file

.. _index:


===========
pyiron_base
===========

.. image:: https://github.com/pyiron/pyiron/workflows/Python%20package/badge.svg
    :target: https://github.com/pyiron//pyiron/actions
    :alt: Build Status

.. image:: https://anaconda.org/conda-forge/pyiron/badges/downloads.svg

.. image:: https://anaconda.org/conda-forge/pyiron/badges/latest_release_date.svg


pyiron_base - an integrated development environment (IDE) for computational materials science. It combines several tools
in a common platform:

• Hierarchical data management – interfacing with storage resources like SQL and `HDF5 <https://support.hdfgroup.org/HDF5/>`_.

• Object oriented job management – for scaling complex simulation protocols from single jobs to high-throughput simulations.

pyiron (called pyron) is developed in the `Computational Materials Design department <https://www.mpie.de/CM>`_ of
`Joerg Neugebauer <https://www.mpie.de/person/43010/2763386>`_ at the `Max Planck Institut für Eisenforschung (Max
Planck Institute for iron research) <https://www.mpie.de/2281/en>`_. While its original focus was to provide a framework
to develop and run complex simulation protocols as needed for ab initio thermodynamics it quickly evolved into a
versatile tool to manage a wide variety of simulation tasks. In 2016 the `Interdisciplinary Centre for Advanced
Materials Simulation (ICAMS) <http://www.icams.de>`_ joined the development of the framework with a specific focus on
high throughput applications. In 2018 pyiron was released as open-source project.

.. note::

    **pyiron_base**: This is the documentation page for the basic infrastructure moduls of pyiron.  If you're new to
    pyiron and want to get an overview head over to `pyiron <https://pyiron.readthedocs.io/en/latest/>`_.  If you're
    looking for the API docs of the atomistic simulation packages check `pyiron_atomistics
    <https://pyiron_atomistics.readthedocs.io/en/latest/>`_.

**************
Explore pyiron
**************
We provide various options to install, explore and run pyiron:

* :ref:`Workstation Installation (recommeded) <InstallLocal>`: for Windows, Linux or Mac OS X workstations (interface for local VASP executable, support for the latest jupyterlab based GUI)

* :ref:`Mybinder.org (beta) <InstallBinder>`: test pyiron directly in your browser (no VASP license, no visualization, only temporary data storage)

* :ref:`Docker (for demonstration) <InstallDocker>`: requires Docker installation (no VASP license, only temporary data storage)

********************
Join the development
********************
Please contact us if you are interested in using pyiron:

* to interface your simulation code or method

* implementing high-throughput approaches based on atomistic codes

* to learn more about method development and Big Data in material science.

Please also check out the `pyiron contributing guidelines <source/developers.html>`_

******
Citing
******
If you use pyiron in your research, please consider citing the following work:

.. code-block:: bibtex

  @article{pyiron-paper,
    title = {pyiron: An integrated development environment for computational materials science},
    journal = {Computational Materials Science},
    volume = {163},
    pages = {24 - 36},
    year = {2019},
    issn = {0927-0256},
    doi = {https://doi.org/10.1016/j.commatsci.2018.07.043},
    url = {http://www.sciencedirect.com/science/article/pii/S0927025618304786},
    author = {Jan Janssen and Sudarsan Surendralal and Yury Lysogorskiy and Mira Todorova and Tilmann Hickel and Ralf Drautz and Jörg Neugebauer},
    keywords = {Modelling workflow, Integrated development environment, Complex simulation protocols},
  }


.. toctree::
   :hidden:

   source/about.rst
   source/installation.rst
   source/faq.rst
   source/examples.rst
   Team <https://pyiron.org/team/>
   Collaborators <https://pyiron.org/collaborators/>
   source/commandline.rst
   source/hdf5.rst
   Contributing <https://pyiron.readthedocs.io/en/latest/source/developers.html>
   Citing <https://pyiron.readthedocs.io/en/latest/source/citation.html>
   License (BSD) <https://github.com/pyiron/pyiron/blob/master/LICENSE>
   source/indices.rst
   Imprint <https://www.mpie.de/impressum>
   Data protection <https://www.mpie.de/3392182/data-protection>
