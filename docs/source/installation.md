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
Login to the remote HPC using secure shell (SSH): 
```
ssh remote_hpc
```
This is achieved by creating an SSH key on the workstation using the `ssh-keygen` command, copying the SSH public key to
the remote computer and adding it to the `authorized_keys`. A typical SSH setup is explained on the internet in various
places for example [cyberciti.biz](https://www.cyberciti.biz/faq/how-to-set-up-ssh-keys-on-linux-unix/). 

#### Connect to Jupyterlab on Remote HPC
In addition, the SSH setup can be configured in the `~/.ssh/config` in the home directory of your workstation you use to
connect to the remote HPC:
```
Host remote_hpc
    Hostname login.hpc.remote.com
    User pyiron
    IdentityFile ~/.ssh/id_rsa
    LocalForward 9000 localhost:8888
```
The `IdentityFile` line defines the SSH key and the `LocalForward` part defines the port forwarding. The port forwarding
is not necessary, still it enables starting a jupyter environment on the remote HPC and connecting to it from the local
workstation.

To test the SSH configuration, login to the remote HPC using `ssh remote_hpc` and then start the jupyter lab environment:
```
jupyter lab
```
Finally, on your local workstation access `http://localhost:9000`. The login requires a security token, this is printed
on the command line of the remote HPC after executing the `jupyter lab` command. 

#### Configure pyiron on Remote HPC
Create a `~/.pyiron` file in the home directory of the remote HPC. As most HPC clusters do not provide a central SQL 
database and file based databases like [SQLite](https://www.sqlite.org) do not work on shared file systems when multiple
compute nodes try to access it at the same time the recommended setup is to disable the database on the HPC cluster.  
```
[DEFAULT]
RESOURCE_PATHS = /home/<username>/resources
PROJECT_CHECK_ENABLED = False
DISABLE_DATABASE = True
```
To disable the database set the `DISABLE_DATABASE` parameter to true and the `PROJECT_CHECK_ENABLED` to false. Finally,
the `RESOURCE_PATHS` is required to store the configuration of the queuing system. 

In the `RESOURCE_PATHS` create a `queues` folder:
```
mkdir -p /home/<username>/resources/queues
```
Afterwards in the queues folder create the configuration for the queuing system following the documentation of the [python
simple queuing system](https://pysqa.readthedocs.io). For example for a SLURM based queuing system create a `queue.yaml`
file inside the `queues` folder. 
```
queue_type: SLURM
queue_primary: slurm
queues:
  slurm: {cores_max: 100, cores_min: 10, run_time_max: 259200, script: slurm.sh}
```
The `queue.yaml` file defines the limits in terms of the maximum number of cores available on a given queue `cores_max`
as well as the minimum number of cores `cores_min`, the maximum run time `run_time_max` and finally the shell script 
template `script` to define the submission script. The submission script `slurm.sh` is again placed in the `queues` 
folder. 
```
#!/bin/bash
#SBATCH --output=time.out
#SBATCH --job-name={{job_name}}
#SBATCH --chdir={{working_directory}}
#SBATCH --get-user-env=L
#SBATCH --partition=slurm
{%- if run_time_max %}
#SBATCH --time={{ [1, run_time_max // 60]|max }}
{%- endif %}
{%- if memory_max %}
#SBATCH --mem={{memory_max}}G
{%- endif %}
#SBATCH --cpus-per-task={{cores}}

{{command}}
```
The shell script for the [python simple queuing system](https://pysqa.readthedocs.io) uses [jinja](https://jinja.palletsprojects.com/)
templates to simplify the configuration of queuing systems. The configuration for other queuing systems is documented 
on the [python simple queuing system documentation](https://pysqa.readthedocs.io/en/latest/queue.html).

#### Validate the Configuration
To test your queuing system configuration use the [python simple queuing system](https://pysqa.readthedocs.io) interface:
```python
from pysqa import QueueAdapter
qa = QueueAdapter(directory="/home/<username>/resources/queues")
print(qa.queue_list)
```
If the `queue.yaml` file is configured correctly, the list of queues available on the remote HPC is plotted when the 
code above is executed. 

To validate the `~/.pyiron` configuration file, after the `queue.yaml` file is correctly configured you can use: 
```python
from pyiron_base import state
print(state.settings._configuration)
```
The `state` object is commonly used to represent the `~/.pyiron` configuration on the python side. 

Furthermore is the `RESOURCE_PATHS` is correctly configured, the `pyiron_base` workflow manager should be able to access
the queuing system configuration: 
```python
from pyiron_base import Project

pr = Project(path=".")
job = pr.create.job.ScriptJob(job_name="test")
print(job.server.queue_list)
```
This should again print the list of queues configured in the `queue.yaml` file. 

Finally, you can try to submit a calculation to the queuing system. 

### Connect to Remote pyiron Installation
After the HPC cluster is configured and it is possible to submit calculation to the queuing system when being directly
logged in on the HPC, the next step is to use pyiron to connect to the remote HPC via SSH. For this kind of setup we 
have to adjust the configuration on the local workstation. Starting with the `~/.pyiron` configuration in the home 
directory of the workstation: 
```
[DEFAULT]
PROJECT_CHECK_ENABLED = False
FILE = /home/<username>/resources/pyiron.db
RESOURCE_PATHS = /home/<username>/resources
```
In addition to the `~/.pyiron` configuration on the local workstation, also a `queue.yaml` configuration is required in
the resource folder on the local workstation. First the configuration folder is created using: 
```
mkdir -p /home/<username>/resources/queues
```
In the queues folder a remote configuration file again named `queue.yaml` is created with the following content. 
```
queue_type: REMOTE
queue_primary: slurm
ssh_host: hpc-cluster.university.edu
ssh_username: remote_user
known_hosts: ~/.ssh/known_hosts
ssh_key: ~/.ssh/id_rsa
ssh_remote_config_dir: /home/<username>/resources/queues/
ssh_remote_path: /home/<username>/
ssh_local_path: /home/<username>/
ssh_continous_connection: True
queues:
    slurm: {cores_max: 100, cores_min: 10, run_time_max: 259200}
```
The configuration on the workstation has to reflect the configuration on the remote HPC cluster. So following the example
in the previous section, the settings for the `queue_type`, the `queue_primary` and the `queues` are similar. Still by
setting the `queue_type` to `REMOTE` the [python simple queuing system](https://pysqa.readthedocs.io) is aware that it
is connecting to a remote HPC. In addition, the `script` parameter for the queue configuration is no longer required as
the submission script templates are not stored on the local workstation but rather just configured on the remote HPC. 

The additional settings define the SSH connection, these settings are similar to the settings in the `~/.ssh/config`. 
This consists of the `ssh_host`, the `ssh_username`, the `known_hosts` and the `ssh_key`. Finally, a few more settings 
are required on the one hand the local path `ssh_local_path` has to be matched to a remote path `ssh_remote_path` to be
able to copy calculation from the local workstation to the remote HPC. Also the configuration of the remote installation
of the [python simple queuing system](https://pysqa.readthedocs.io) is required, this can be set using `ssh_remote_config_dir`.

Furthermore, by specifying the `ssh_continous_connection` variable to be true, only a single SSH connection is established
and the connection remains open for the duration the pyiron session is active on the local workstation. An alternative
option is to set this parameter to false. In that case a new SSH connection is established for every command, this is 
slower but does not require a continuous internet connection on the workstation. Disabling the `ssh_continous_connection`
is not recommended, it is only designed for use cases when a stable internet connection is not available. 

### Advanced configuration 