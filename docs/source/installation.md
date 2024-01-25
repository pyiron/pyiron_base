# Installation

## Try pyiron_base online

To quickly test `pyiron_base` in your web browser you can use the [MyBinder](https://mybinder.org/v2/gh/pyiron/pyiron_base/main?urlpath=lab) 
environment. MyBinder is a cloud environment, so the resources are limited and there is no permanent data storage: 

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pyiron/pyiron_base/HEAD?urlpath=lab)

## Workstation Installation
By default, only the installation of the `pyiron_base` python package is required for a workstation. In this case 
`pyiron_base` is using the [SQLite](https://www.sqlite.org) backend and places a `pyiron.db` database file in the users 
home directory. More advanced configuration options are discussed below as part of the HPC installation.  

### Conda Package Manager 
The recommended way to install `pyiron_base` independent of the infrastructure is using the `conda` package manager:

```commandline
conda install -c conda-forge pyiron_base
```

The conda package is provided via the [conda-forge](https://conda-forge.org) community channel. 

### Pip Package Manager 
An alternative way to install `pyiron_base` is via the `pip` package manager: 

```commandline
pip install pyiron-base
```

In contrast to the conda package manager the pip package manager provides both binary packages and packages consisting 
of source code, which is automatically complied locally. The founder of anaconda wrote a [blog post](http://technicaldiscovery.blogspot.com/2018/03/reflections-on-anaconda-as-i-start-new.html) 
about this topic back in 2018 and the challenges of locally compiled versions of numerical libraries like `numpy` and
`scipy`. 

### Docker Installation 
While a direct installation via `conda` and alternatively via `pip` is recommended, `pyiron_base` is also provided as 
Docker container. The docker image can be installed using the following command:

```commandline
docker pull pyiron/base:latest
```

To start the jupyter notebook inside the docker container the following command can be used: 

```commandline
docker run -i -t -p 8888:8888 pyiron/base /bin/bash -c "source /opt/conda/bin/activate; jupyter notebook --notebook-dir=/home/jovyan/ --ip='*' --port=8888"
```

Still as this is a container solution, the integration with the operating system is limited.

## HPC Cluster Installation 
In analogy to the workstation installation also the HPC cluster installation requires the installation of the `pyiron_base`
python package. Again this can be done either via `conda` or via `pip`. In addition, the connection to the queuing system
of an HPC system requires some additional configuration. This configuration can be achieved either via a configuration 
file `~/.pyiron` or via environment variables. The configuration with an environment file is recommended for persistence.

In the following three sections are introduced: 
* Installation of `pyiron_base` on the login node of the HPC cluster with direct access to the queuing system. 
* Connecting a local `pyiron_base` instance on a workstation to a `pyiron_base` instance on an HPC cluster via SSH. 
* Advanced configuration settings including the configuration of a PostgreSQL database for optimized performance.  

It is highly recommended to follow these sections step by step, as already a typo at this stage of the configuration can
lead to rather cryptic error messages. 

### Remote pyiron Installation

### Connect to Remote pyiron Installation

### Advanced configuration 