.. _installation:

============
Installation
============

******************
Conda Installation
******************

The recommended way to install pyiron is via the conda package manager in a Linux environment. So if you are using
Windows we recommend installing the `Windows subsystem for Linux
<https://docs.microsoft.com/en-us/windows/wsl/install-win10>`_ before you install pyiron and if you are on macOS X we
recommend using a `virtual machine/ virtual box <https://www.virtualbox.org>`_. Native installations on both Windows and
macOS X are possible but are restricted to molecular dynamics calculations with interatomic potentials and do not
support density functional theory(DFT) codes. We collaborate with the open-source community at `conda-forge
<https://conda-forge.org>`_ to not only provide the pyiron package via their community channel, but also executables for
compatible simulation codes like `GPAW <https://wiki.fysik.dtu.dk/gpaw/>`_, `LAMMPS <https://lammps.sandia.gov>`_ and
`S/PHI/nX <https://sxrepo.mpie.de>`_ and their parameter files like pseudopotentials and interatomic potentials. To get
started you can install pyiron using:

.. code-block:: bash

    conda install -c conda-forge pyiron_base


**********************
Advanced Configuration
**********************

While the conda-based installation is usually sufficient for workstation installations to get started with pyiron, it
can be extended to support your own executables, include your own parameter files, support commercial codes like `VASP
<https://www.vasp.at>`_ or updating the database performance by switching from `SQLite <https://www.sqlite.org>`_ to
`PostgreSQL <https://www.postgresql.org>`_. 

Custom Executables and Parameter Files
======================================

pyiron can either be configured using a configuration file named :code:`~/.pyiron` located in the user's home directory
or by specifying environment variables. The options are similar either way, so we start with the configuration file. The
default configuration file pyiron assumes if it does not find a configuration file is:

.. code-block:: bash

    [DEFAULT]  
    PROJECT_CHECK_ENABLED = False
    FILE = ~/pyiron.db
    RESOURCE_PATHS = ${CONDA_PREFIX}/share/pyiron

The first line :code:`[DEFAULT]` defines the current configuration to overwrite the default configuration. The second
line :code:`PROJECT_CHECK_ENABLED` disables the project check which enables pyiron to write to the whole file system.
The third lines defines the object index to be stored in an SQLite database file :code:`FILE` which is located in the
home directory :code:`~/pyiron.db`. It is important to copy the database in case you change the configuration otherwise
existing calculation are lost. Finally the :code:`RESOURCE_PATHS` provides the path to the parameter files. Inside
pyiron you can check the current configuration using: 

.. code-block:: python

    from pyiron_base import Settings
    s = Settings()
    s._configuration

Below, the individual options are explained one by one:

* the :code:`[DEFAULT]` option defines the current :code:`~/.pyiron` configuration to overwrite the default
  configuration.

* the :code:`RESOURCE_PATHS` option defines the resource path is a list of :code:`;` separated paths where pyiron checks
  for resource files. A template of such a resource directory is available on `github
  <https://github.com/pyiron/pyiron-resources>`_ and it can be downloaded as an archive from the `release page
  <https://github.com/pyiron/pyiron-resources/releases>`_. We recommend to create a folder :code:`~/pyiron/resources`
  and store the parameter files and links to the executables there. The links are basically shell scripts which can be
  modified to load modules. By default the conda path is added, therefore there is no need to add it manually. 

* the :code:`PROJECT_PATHS` option is similar to the resource path but for storing simulation protocols rather than
  parameter files. When the :code:`PROJECT_CHECK_ENABLED` option is set to :code:`true` then the read and write access
  within pyiron is limited to the directories defined in the :code:`PROJECT_PATHS`. Again multiple directories can be
  separated by :code:`;`. An alternative but outdated name for this option is :code:`TOP_LEVEL_DIRS`. 

Besides the general variables in the :code:`~/.pyiron` configuration, the other settings are used to define the database
connection. More detailed examples about the configuration can be found below; for now we continue with the
configuration of the database. pyiron can use a database to build an index of the HDF5 files on the file system which
accelerates job analysis. By default pyiron uses an `SQLite <https://www.sqlite.org>`_ database for this index, but the
database can also be disabled or a `PostgreSQL <https://www.postgresql.org>`_ database can be used to improve
performance. 

* By default the database is defined by the :code:`FILE` option which is equal to the :code:`DATABASE_FILE` option and
  gives the path to the `SQLite <https://www.sqlite.org>`_ database file. As the `SQLite <https://www.sqlite.org>`_
  database is a file-based database, it struggles with parallel access on a shared file system (common for HPC
  clusters). 

* To address this limitation it is possible to disable the database on HPC clusters using the :code:`DISABLE_DATABASE`
  option by setting it to :code:`true`. This is commonly used when the calculations are only executed on the remote
  cluster but the analysis is done on a local workstation or a group server which supports an SQL-based database.

* The other database options, namely :code:`TYPE`, :code:`HOST`, :code:`NAME`, :code:`USER`, :code:`PASSWD` and
  :code:`JOB_TABLE` define the connection details to connect to a PostgreSQL database. Inside pyiron `sqlalchemy
  <https://www.sqlalchemy.org>`_ is used to support different SQL-based databases, therefore it is also possible to
  provide the sqlalchemy connection string directly as :code:`CONNECTION`. 

* Finally some pyiron installations use a group management component which is currently in development. They might have
  additional options in their :code:`~/.pyiron` configuration to enable sharing calculations between different users.
  These options are :code:`VIEWERUSER`, :code:`VIEWERPASSWD` and :code:`VIEWER_TABLE`. As this is a development feature
  it is not yet fully documented. Basically those are the access details for the global database viewer, which can read
  the database entries of all users. With this configuration it is possible to load jobs of other users. 

In analogy to the :code:`~/.pyiron` configuration file pyiron also supports using environment variables to configure the
pyiron installation. The available environment variables are: 

* the :code:`PYIRONCONFIG` environment variable defines the location of the :code:`.pyiron` configuration file. 

* the :code:`PYIRONRESOURCEPATHS` environment variable defines the :code:`RESOURCE_PATHS` option.

* the :code:`PYIRONPROJECTPATHS` environment variable defines the :code:`PROJECT_PATHS` option.

* the :code:`PYIRONPROJECTCHECKENABLED` environment variable defines the :code:`PROJECT_CHECK_ENABLED` option.

* the :code:`PYIRONDISABLE` environment variable defines the :code:`DISABLE_DATABASE` option.

* the :code:`PYIRONSQLTYPE`, :code:`PYIRONSQLFILE`, :code:`PYIRONSQHOST`, :code:`PYIRONSQLDATABASE`, :code:`PYIRONUSER`
  and :code:`PYIRONSQLUSERKEY` environment varaibles define the SQL database connection and can also be summarized in
  the :code:`PYIRONSQLCONNECTIONSTRING` environment variable. 

* the :code:`PYIRONSQLVIEWTABLENAME`, :code:`PYIRONSQLVIEWUSER` and :code:`PYIRONSQLVIEWUSERKEY` environment variables
  define the SQL viewer connection and can also be summarized in the :code:`PYIRONSQLVIEWCONNECTIONSTRING` environment
  variable. 

To further explain the usage of the different parameters, we discuss common use cases in the following:

PostgreSQL Database
===================

To accelerate the pyiron installation it is recommended to use a `PostgreSQL <https://www.postgresql.org>`_ database
rather than the default `SQLite <https://www.sqlite.org>`_ database. To configure the database server, the following
options can be added to the :code:`~/.pyiron`:

* :code:`TYPE` the typ of the database, while `sqlalchemy <https://www.sqlalchemy.org>`_ supports a wide range of
  differnet databases `PostgreSQL <https://www.postgresql.org>`_ is recommended and can be selected by setting the type
  to :code:`Postgres`. 

* :code:`HOST` the database host where the database is running. 

* :code:`NAME` the name of the database.

* :code:`USER` the database user, in contrast to many other software packages pyiron requires one database user per
  system user who is using pyiron. The database is only used to store an index of the calculations executed with pyiron,
  therefore knowledge gained from accessing the database is limited unless the user has also access to the file system. 

* :code:`PASSWD` the database user password. While it is a bad practice to store the database password in the
  configuration file, the database only contains the the job index. Still it is important that the user creates an
  pyiron specific password and should never store their system user password in the :code:`.pyiron` configuration file. 

* :code:`JOB_TABLE` the name of the database table. pyiron is commonly using one table per user. 

A typical :code:`.pyiron` configuration with a `PostgreSQL <https://www.postgresql.org>`_ database might look like this: 

.. code-block:: bash

    [DEFAULT]  
    TYPE = Postgres
    HOST = database.hpc-cluster.university.edu
    NAME = pyiron
    USER = janj
    PASSWD = **********
    JOB_TABLE = jobs_janj
    PROJECT_PATHS = ~/pyiron/projects
    RESOURCE_PATHS = ~/pyiron/resources  

Be careful when updating the database configuration as pyiron does not transfer the content of the database
automatically.

Remote HPC Cluster
==================

While the previous section discussed the installation of pyiron on a local workstation, the following section discusses
how to configure a remote HPC cluster to transfer jobs to the HPC cluser for execution and back for analysis. For
setting up pyiron on an HPC cluster there are basically three different configurations available: 

* Install pyiron on the HPC cluster, with `jupyterhub <https://jupyterhub.readthedocs.io>`_ running as a central service
  on the login node using the `sudospawner <https://github.com/jupyterhub/sudospawner>`_ to authorize users. In this
  configuration the user only needs a web browser and all simulation results will remain on the HPC cluster. The
  limitation of this approach is that both the global `PostgreSQL <https://www.postgresql.org>`_ database as well as the
  `jupyterhub <https://jupyterhub.readthedocs.io>`_ have to be running on the cluster with the `PostgreSQL
  <https://www.postgresql.org>`_ database being accessible from all compute nodes. 

* The second configuration is running pyiron on the HPC without the `jupyterhub <https://jupyterhub.readthedocs.io>`_ or
  a database, and storing the simulation results on a group server. Servers in the research group are commonly less
  strictly governed, so installing the `jupyterhub <https://jupyterhub.readthedocs.io>`_ on the group server as well as
  the `PostgreSQL <https://www.postgresql.org>`_ database for faster data analysis should be possible in most cases.
  From the user perspective the setup still only requires a web browser on the user's end device, and leaves the task of
  backing up the simulation data on the group server side rather than the end-user. 

* Finally the third configuration is the workstation installation, with a `PostgreSQL <https://www.postgresql.org>`_
  database or even just a `SQLite <https://www.sqlite.org>`_ file based database with using the HPC cluster only to
  execute the calculation and copying the simulation results to local workstation after every calculation. 

We start by explaining the first configuration and then build on top of this setup to add the remote transfer
capabilities. 

HPC Cluster with PostgreSQL Database and Jupyterhub
---------------------------------------------------

The :code:`~/.pyiron` is structured just like a workstation installation with a `PostgreSQL
<https://www.postgresql.org>`_ database as explained above. In addition to the previous resource directories we add
another subfolder in the resource directory to configure the queuing system using `pysqa
<https://github.com/pyiron/pysqa>`_ as queuing system adapter. `pysqa <https://github.com/pyiron/pysqa>`_ is based on
the idea of using shell script based templates to configure the different queues as modern queuing sytem provide a wide
range of settings but most users commonly submit their jobs with very similar settings. We discuss a sample
configuration for `SLURM <https://slurm.schedmd.com/documentation.html>`_ sample configurations for other queuing
systems are available on `Github <https://github.com/pyiron/pysqa/tree/master/tests/config>`_.

.. code-block:: bash

    resources/
      queues/
        queue_1.sh  
        queue_2.sh
        queue.yaml

The queues directory contains one :code:`queue.yaml` configuration file and multiple `jinja
<https://jinja.palletsprojects.com>`_ based shell script templates for submitting jobs. These templates define a
commonly used set of parameters used to submit calculations, it can contain a restriction on a specific queue or
partition but it does not have to. A typical queue template that might be used in :code:`queue_1.sh` and
:code:`queue_2.sh` is shown below:

.. code-block:: bash

    #!/bin/bash
    #SBATCH --output=time.out
    #SBATCH --job-name={{job_name}}
    #SBATCH --workdir={{working_directory}}
    #SBATCH --get-user-env=L
    #SBATCH --partition=slurm
    {%- if run_time_max %}
    #SBATCH --time={{run_time_max // 60}}
    {%- endif %}
    {%- if memory_max %}
    #SBATCH --mem={{memory_max}}
    {%- endif %}
    #SBATCH --cpus-per-task={{cores}}

    {{command}}

Such a template contains the variables :code:`{{job_name}}` which is used to identify the job on the queuing system.
Typically, pyiron job names are constructed using the prefix :code:`pi` followed by the pyiron job id. This allows
pyiron to match the job on the queuing system with the job table. The second option is the :code:`{{working_directory}}`
which is the directory where the job is located and the simulation code is executed. For pyiron this is typically a
subdirectory of the simulation protocol to simplify identifiying broken calculation on the filesystem. The third option
is the :code:`run_time` which specifies the run time in seconds, followed by the :code:`memory_max` which specifies the
memory requirement of a given calculation. Both parameters are optional. Finally the :code:`cores` defines the number of
CPU cores used for a calculation and the :code:`command` parameter is set by pyiron to load a pyiron object during the
execution. When a pyiron job is executed on a compute node, a python process is first called to reload the pyiron object
and then the pyiron object calls the shell script just like a regular job executed on the login node. By initially
calling a python process, pyiron is able to track the progress of the calculation.

Besides the queue templates, the queues directory also contains the queue configuration :code:`queue.yaml`: 

.. code-block:: bash

    queue_type: SLURM
    queue_primary: queue_one
    queues:
      queue_one: {cores_max: 40, cores_min: 1, run_time_max: 3600, script: queue_1.sh}
      queue_two: {cores_max: 1200, cores_min: 40, run_time_max: 345600, script: queue_2.sh}

The queue configuration defines the limits of the individual queues which helps the user to select the appropriate queue
for their simulation. The :code:`queue_type` defines the type of the queuing system, the :code:`queue_primary` defines
the primary queue and finally :code:`queues` defines the available queues. Typically each queue is associated with a
shell script template, like in this case :code:`queue_one` is associated with :code:`queue_1.sh` and :code:`queue_two`
is associated with :code:`queue_2.sh`. Additional queue configuration templates are available on `Github
<https://github.com/pyiron/pysqa/tree/master/tests/config>`_.

Submit to Remote HPC
--------------------

Submitting calculations to a remote HPC requires some light configuration. On the HPC, disable the database in the
:code:`.pyiron` with the following lines:

.. code-block:: bash

    [DEFAULT]  
    DISABLE_DATABASE = True
    PROJECT_PATHS = ~/pyiron/projects
    RESOURCE_PATHS = ~/pyiron/resources  

Then configure the remote HPC just like a regular HPC by adding the queuing system configuration as described above. It
is recommended to test the submission on the remote HPC before configuring the datatransfer. 

On the system that will be used to submit calculations to the remote HPC (e.g. your laptop or an in-between login
machine), create the queues directory in the resource path, containing only the queue configuration:

.. code-block:: bash

    resources/
      queues/
        queue.yaml

This queue configuration now includes additional options to handle the SSH connection to the remote cluster: 

.. code-block:: bash

    queue_type: REMOTE
    queue_primary: queue_one
    ssh_host: hpc-cluster.university.edu
    ssh_username: janj
    known_hosts: ~/.ssh/known_hosts
    ssh_key: ~/.ssh/id_rsa
    ssh_remote_config_dir: /u/share/pyiron/resources/queues/
    ssh_remote_path: /u/janj/remote/
    ssh_local_path: /home/jan/pyiron/projects/
    ssh_continous_connection: True
    queues:
      queue_one: {cores_max: 40, cores_min: 1, run_time_max: 3600}
      queue_two: {cores_max: 1200, cores_min: 40, run_time_max: 345600}

The :code:`ssh_host` defines the name of the login node, with :code:`ssh_username` the user on the remote machine and
:code:`known_hosts` and :code:`ssh_key` the local configuration files to connect to the remote host. Currently pyiron
only supports ssh key based authentification for remote calculation. By setting :code:`ssh_continous_connection`, the
same connection is reused for data transfers which is commonly more efficient than creating individual connections for
each command. Still, this assumes that the connection between the workstation or group server and the remote HPC cluster
is stable. If this is not the case - for example, when using a mobile connection - it is recommended to disable this
option. The :code:`ssh_remote_config_dir` defines the configuration of the queuing system on the remote cluster. Finally
the calculations are copied from the local directory :code:`ssh_local_path` to the remote directory
:code:`ssh_remote_path`. In the above example, if a calculation is submitted in the directory
:code:`/home/jan/pyiron/projects/first/subproject` then the files are copied to :code:`/u/janj/remote/first/subproject`.
By retaining the path when transfering the files it is easier to debug failed calculations. Finally the queues are
defined locally to have quick access to the queue configurations, but it is not necessary to define the submission
templates as those are available on the remote machine. In addition the other resources have to be identical on both
systems. The easiest way to achieve this is to copy the resource directory once the installation is working on the
remote machine.

Submit to multiple Remote HPC Clusters
--------------------------------------

Finally pyiron also supports configuring multiple HPC clusters. In this case rather than creating a :code:`queue.yaml`
file in the queues resource directory we create a :code:`clusters.yaml` file with the following content: 

.. code-block:: bash

    cluster_primary: cluster_one
    cluster:
      cluster_one: cluster_1.yaml
      cluster_two: cluster_2.yaml

The :code:`cluster_primary` defines the default cluster and the different clusters are each defined in their own
:code:`cluster_*.yaml` file. Those :code:`cluster_*.yaml` have the same structure as the :code:`queue.yaml` file
discussed above, but they cannot be named :code:`queue.yaml` as pyiron otherwise assumes that only one cluster is
available.

********************************
Alternative Installation Options
********************************

So far we discussed the installation of pyiron on an individual workstation via conda or on a HPC cluster. In the
following we focus on developer-specific setups to install pyiron directly from its source. It is recommended to start
with a conda installation and then replace only the pyiron version so that conda can still automatically manage all
dependencies/environment settings for you. In case this is not possible, e.g. if conda is not allowed on your HPC
cluster, then pyiron can be installed directly from the source code.

Install from Source
===================

For development, it is recommended to first create a conda environment containing all of pyiron's dependencies. The
dependencies are available in pyiron's `environment.yml
<https://github.com/pyiron/pyiron/blob/master/.ci_support/environment.yml>`_ file.

.. code-block:: bash
    git clone https://github.com/pyiron/pyiron.git
    conda env create -f pyiron/environment.yml

If conda is not available on your machine, the next best thing would be to install pyiron and its dependencies via pip.

Using pip
---------

The default installation via pip installs the latest release version of pyiron. So in case your HPC cluster does not
support installing pyiron via conda you can install this release version via pip and then continue with the setup of
your remote HPC cluster as described above.

.. code-block:: bash

    pip install pyiron

For those who want to test the nightly releases of pyiron which include the latest status of the master branch you can
install those via pip as well: 

.. code-block:: bash

    pip install --pre pyiron

Using git
---------

To get the latest pyiron version and access changes on development branches pyiron can also be installed via git. For
example you can download the pyiron sourcecode to :code:`~/pyiron/software` using:

.. code-block:: bash

    git clone https://github.com/pyiron/pyiron.git ~/pyiron/software

Based on the previous workstation setup your :code:`~/pyiron` directory should contain the following folders:

.. code-block:: bash

   pyiron/
     projects/
     resources/
     software/

To include this version in your :code:`PYTHONPATH` add the following line to your :code:`~/.profile` or
:code:`~/.bashrc` configuration:

.. code-block:: bash

    export PYTHONPATH=${HOME}/pyiron/software/:${PYTHONPATH}

When you import pyiron in any python shell or jupyter notebook it should load the version from
:code:`~/pyrion/software`. Finally you can switch to other branches using git: 

.. code-block:: bash

    git checkout -b master

In this case we switch to the master branch.

Download pyiron Parameter Files
===============================

For source code based installations it is also possible to download the pyiron resources directly from within pyiron.
Simply open a python shell and import pyiron:

.. code-block:: python

   > import pyiron
   > pyiron.install()
   >>> It appears that pyiron is not yet configured, do you want to create a default start configuration (recommended: yes). [yes/no]:
   > yes
   > exit()

This command does the following steps in the background:

* Create a :code:`~/.pyiron` config file – with the default settings (for simple installations)

* Create a :code:`~/pyiron/projects` directory – pyiron can only execute calculations within this project directory to
  prevent any interference with other tools or simulation management solutions.

* Create a :code:`~/pyiron/resources` directory – this directory includes the link to the executables and potentials,
  sorted by code.

***************************************
Demonstration and Training Environments
***************************************

For workshops, tutorials, and lectures it is sometimes necessary to setup multiple computers with very similar
configurations, and - depending on the conference location - internet access might be limited. For these cases pyiron
provides setup instructions for demonstration and training environments.

Cloud Solutions
===============

You can test pyiron on `Mybinder.org (beta) <https://mybinder.org/v2/gh/pyiron/pyiron/master?urlpath=lab>`_, without the
need for a local installation. It is a flexible way to get a first impression of pyiron but it does not provide any
permanent storage by default. Loading the pyiron environment on mybinder can take 5 to 15 minutes in case a new docker
container needs to be built. Mybinder is a free service, so sessions on its servers are limited in duration and memory
limits, and their stability is not guaranteed. We recommend having a backup plan when using mybinder for
presentations/interactive tutorials, since the mybinder instance might be shutdown if it is idle for too long. 

Docker Container
================

For demonstration purposes we provide Docker containers on `Dockerhub <https://hub.docker.com/r/pyiron/pyiron/>`_ these
can be downloaded and executed locally once docker is installed. Again, these container images do not provide any
permanent storage, so all information is lost once the docker container is shut down. To download the docker container
use: 

.. code-block:: bash

    docker pull pyiron/pyiron:latest

After downloading the docker container you can use it either with jupyter notebook:

.. code-block:: bash

    docker run -i -t -p 8888:8888 pyiron/pyiron /bin/bash -c "source /srv/conda/envs/notebook/bin/activate; jupyter notebook --notebook-dir=/home/pyiron/ --ip='*' --port=8888"

or with jupyter lab:

.. code-block:: bash

    docker run -i -t -p 8888:8888 pyiron/pyiron /bin/bash -c "source /srv/conda/envs/notebook/bin/activate; jupyter lab --notebook-dir=/home/pyiron/ --ip='*' --port=8888"

After the run command the following line is displayed. Copy/paste this URL into your browser when you connect for the
first time, to login with a token:

.. code-block:: bash

    http://localhost:8888/?token=<your_token>

Open the link with your personal jupyter token :code:`<your_token>` in the browser of your choice. Just like the Binder
image, the Docker image comes with several pyiron examples preinstalled.

Install Utility
===============

To setup a local lab with pyiron when the internet connection is limited, we provide a classical installer for Windows,
macOS X and Linux which is based on the `conda constructor <https://github.com/conda/constructor>`_. If you do not have
anaconda installed you can download this installer and get started with just a single `download
<https://github.com/pyiron/pyiron-installer/releases>`_.

.. toctree::
   :maxdepth:2
