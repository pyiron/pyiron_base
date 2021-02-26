pyiron
======

.. image:: https://coveralls.io/repos/github/pyiron/pyiron_base/badge.svg?branch=master
    :target: https://coveralls.io/github/pyiron/pyiron_base?branch=master
    :alt: Coverage Status

.. image:: https://anaconda.org/conda-forge/pyiron_base/badges/latest_release_date.svg
    :target: https://anaconda.org/conda-forge/pyiron_base/
    :alt: Release_Date

.. image:: https://github.com/pyiron/pyiron_base/workflows/Python%20package/badge.svg
    :target: https://github.com/pyiron//pyiron_base/actions
    :alt: Build Status

.. image:: https://anaconda.org/conda-forge/pyiron_base/badges/downloads.svg
    :target: https://anaconda.org/conda-forge/pyiron_base/
    :alt: Downloads

.. image:: https://readthedocs.org/projects/pyiron-base/badge/?version=latest
    :target: https://pyiron-base.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

pyiron - an integrated development environment (IDE) for computational materials science. While the general pyiron framework is focused on atomistic simulations, pyiron_base is independent of atomistic simulation. It can be used as a standalone workflow management combining a hierachical storage interface based on HDF5, support for HPC computing clusters and a user interface integrated in the Jupyter environment. 

Installation
------------
You can test pyiron on `Mybinder.org (beta) <https://mybinder.org/v2/gh/pyiron/pyiron_base/master?urlpath=lab>`_.
For a local installation we recommend to install pyiron inside an `anaconda <https://www.anaconda.com>`_  environment::

    conda install -c conda-forge pyiron_base

See the `Documentation-Installation <https://pyiron.github.io/source/installation.html>`_ page for more details.

Example
-------
After the successful configuration you can start your first pyiron calculation. Navigate to the the projects directory and start a jupyter notebook or jupyter lab session correspondingly::

    cd ~/pyiron/projects
    jupyter notebook

Open a new jupyter notebook and inside the notebook you can now validate your pyiron calculation by creating a test project::

    from pyiron import Project
    pr = Project('test')
    pr.path


Getting started:
----------------
Test pyiron with mybinder:

.. image:: https://mybinder.org/badge_logo.svg
     :target: https://mybinder.org/v2/gh/pyiron/pyiron_base/master
     :alt: mybinder


License and Acknowledgments
---------------------------
``pyiron_base`` is licensed under the BSD license.

If you use pyiron in your scientific work, `please consider citing <http://www.sciencedirect.com/science/article/pii/S0927025618304786>`_ ::

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
