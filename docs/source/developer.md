# Developers
Explain the technology behind the `pyiron_base` workflow manager. 

## HDF5 Serialization Architecture

### Structure
Each hierachical object lives under its own group in the hdf, i.e. objects that are attributes of another must have 
their own sub-group in that larger objects group.  In its group each object must store
- `TYPE` equal to `str(type(self))` this provides the module path and class name from which pyiron will load a class
- `NAME` equal to `type(self).__name__` the unqualified class name, informational only

They may also store
- `HDF_VERSION` equal to a version string with format `MAJOR.MINOR.PATCH` the version of the structure of the type *in* 
  *HDF5*; all classes must be able to read from HDF5 with at least the same MAJOR release, but explicit breaking 
  behaviour should be very rare
- `VERSION` equal to a version string with format `MAJOR.MINOR.PATCH` the version of the functionality of the class; 
  higher version must not change the HDF5 structure unless they also change HDF_VERSION

For example a class defined like this
```python
class Foo:
    def __init__(self, parameter):
        self.bar = Bar()
        self.baz = Baz()
        self.parameter = parameter
```
 
should be serialized as
```
foo/
foo/TYPE
foo/NAME
foo/VERSION
foo/HDF_VERSION
foo/parameter
foo/bar/
foo/bar/TYPE
foo/bar/NAME
foo/bar/VERSION
foo/bar/HDF_VERSION
foo/baz/
foo/baz/TYPE
foo/baz/NAME
foo/baz/VERSION
foo/baz/HDF_VERSION
```

### Writing to HDF5
Each type must define a `to_hdf(self, hdf, group_name = None)` method that takes the given `hdf` object, creates a 
subgroup called `group_name` in it (if given) and then serializes itself to this group.  Some objects may keep a default
`ProjectHDFio` object during their lifetime (e.g. jobs), in this case `hdf` maybe an optional parameter.

### Reading from HDF5
Each type must define a `from_hdf(self, hdf, group_name = None)` method and may define a `from_hdf_args(cls, hdf)`.  
`from_hdf()` restores the state of the already initialized object from the information stored in the HDF5 file.
`from_hdf_args()` reads the required parameters to instantiate the object from HDF5 and returns them in a `dict`.

To read an object from a given `ProjectHDFio` path, call the `to_object()` method. This will first call `import_class` 
to read the class object, then `make_from_hdf()` to instantiate it, if the class defines `from_hd_args()` it will be 
called to supply the correct init parameters.  `to_object()` can also be supplied with additional parameters to override 
the ones written to HDF5, in particular it will always provide `job_name` and `project`.  However only those parameters 
that are needed (i.e. declared by that classes' `__init__()`) will be passed.

## Run function 
Explain how a job is executed. 

## Queuing System Submission

If you just want to configure the queue setup, look into the [documentation](https://pyiron.readthedocs.io/en/latest/source/installation.html#hpc-cluster-with-postgresql-database-and-jupyterhub). 
The following details on the code flow for job submission to the queue.

Every time pyiron submits a job to the queue (reachable from the current location - for remote setup this is run on the
remote machine) it runs:

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
(copied from [here](https://github.com/pyiron/pysqa/tree/main/tests/config/slurm/slurm.sh))

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

where the str(executable) or the executable.executable_path point to the shell script for the chosen version as defined 
in the resources. e.g. run multi core LAMMPS 2020.03.03 (run_lammps_2020.03.03_mpi.sh):

```bash
#!/bin/bash
mpiexec -n $1 --oversubscribe lmp_mpi -in control.inp;
```

(copied from [here](https://github.com/pyiron/pyiron-resources/blob/main/lammps/bin/run_lammps_2020.03.03_mpi.sh))

## Command Line
Adding a new sub command is done by adding a new module to `pyiron.cli`. This module needs to define a `register` and a 
`main` function.  The former is called with an `argparse.ArgumentParser` instance as sole argument and should define the
command line interface in the [usual way](https://docs.python.org/3/library/argparse.html). The latter will be called
with the parsed arguments and should just execute whatever it is that utility should be doing.  Additionally, if you 
need to control the `formatter_class` and `epilog` keyword arguments when creating the `argparse.ArgumentParser`
instance you can set the `formatter` and `epilog` toplevel variables (see the `ls` sub command for an example).  
Finally, you must add the module to the `pyiron.cli.cli_modules` dict.
