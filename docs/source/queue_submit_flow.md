# How pyiron submits a job into the queue

Every time pyiron submits a job to the queue (reachable from the current location - for remote setup this is run on the remote machine) it runs:

https://github.com/pyiron/pyiron_base/blob/b1e188458e96ae2fe71591ffef2769748481c204/pyiron_base/jobs/job/runfunction.py#L406-L420

The job submission is handled by the queue adapter which populates the slurm run template 

```bash
#!/bin/bash
{%- if cores < 40 %}
#SBATCH --partition=s.cmfe
{%- else %}
#SBATCH --partition=p.cmfe
{%- endif %}
#SBATCH --ntasks={{cores}}
#SBATCH --constraint='[swi1|swi1|swi2|swi3|swi4|swi5|swi6|swi7|swi8|swi9]'
{%- if run_time_max %}
#SBATCH --time={{ [1, run_time_max // 60]|max }}
{%- endif %}
{%- if memory_max %}
#SBATCH --mem={{memory_max}}G
{%- else %}
{%- if cores < 40 %}
#SBATCH --mem-per-cpu=3GB
{%- endif %}
{%- endif %}
#SBATCH --output=time.out
#SBATCH --error=error.out
#SBATCH --job-name={{job_name}}
#SBATCH --chdir={{working_directory}}
#SBATCH --get-user-env=L

pwd;
echo Hostname: `hostname`
echo Date: `date`
echo JobID: $SLURM_JOB_ID

{{command}}
```
(see https://github.com/eisenforschung/pyiron-resources-mpie/blob/master/queues/cmti.sh)

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

e.g. run single core VASP-5.4.4 (run_vasp_5.4.4.sh):

```bash
#!/bin/bash
module load intel/19.1.0 impi/2019.6
module load vasp/5.4.4-buildFeb20

if [ $(hostname) == 'cmti001' ];
then
        unset I_MPI_HYDRA_BOOTSTRAP;
        unset I_MPI_PMI_LIBRARY;
        mpiexec -n 1 vasp_std
else
        srun -n 1 --exclusive --mem-per-cpu=0 -m block:block,Pack vasp_std
fi
```

https://github.com/eisenforschung/pyiron-resources-mpie/blob/master/vasp/bin/run_vasp_5.4.4.sh
