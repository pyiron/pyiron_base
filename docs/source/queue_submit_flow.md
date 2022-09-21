# How pyiron submits a job into the queue

If you just want to configure the queue setup, look into the [documentation](https://pyiron.readthedocs.io/en/latest/source/installation.html#hpc-cluster-with-postgresql-database-and-jupyterhub). The following details on the code flow for job submission to the queue.

Every time pyiron submits a job to the queue (reachable from the current location - for remote setup this is run on the remote machine) it runs:

https://github.com/pyiron/pyiron_base/blob/b1e188458e96ae2fe71591ffef2769748481c204/pyiron_base/jobs/job/runfunction.py#L406-L420

The job submission is handled by the queue adapter which populates the slurm run template 

```bash
#!/bin/bash
#SBATCH --output=time.out
#SBATCH --job-name={{job_name}}
#SBATCH --workdir={{working_directory}}
#SBATCH --get-user-env=L
#SBATCH --partition=slurm
{%- if run_time_max %}
#SBATCH --time={{ [1, run_time_max // 60]|max }}
{%- endif %}
{%- if memory_max %}
#SBATCH --mem={{memory_max}}G
{%- endif %}
#SBATCH --cpus-per-task={{cores}}
```
(copied from [here](https://github.com/pyiron/pysqa/tree/master/tests/config/slurm/slurm.sh))

and submits this into the queue. I.e. the command running will be        

```python
command = (
            "python -m pyiron_base.cli wrapper -p "
            + job.working_directory
            + " -j "
            + str(job.job_id)
        )
```
which essentially does a `job.load()` and a `job.run()` on the compute node.

The `job.run()` calls finally

https://github.com/pyiron/pyiron_base/blob/b1e188458e96ae2fe71591ffef2769748481c204/pyiron_base/jobs/job/runfunction.py#L488-L513

where the str(executable) or the executable.executable_path point to the shell script for the chosen version as defined in the resources.

e.g. run multi core LAMMPS 2020.03.03 (run_lammps_2020.03.03_mpi.sh):

```bash
#!/bin/bash
mpiexec -n $1 --oversubscribe lmp_mpi -in control.inp;
```

(copied from [here](https://github.com/pyiron/pyiron-resources/blob/master/lammps/bin/run_lammps_2020.03.03_mpi.sh))
