# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Jobclass to execute python scripts and jupyter notebooks
"""

import os
import shutil
from pyiron_base.job.generic import GenericJob
from pyiron_base.generic.parameters import GenericParameters
from pyiron_base.generic.datacontainer import DataContainer

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class ScriptJob(GenericJob):
    """
    The ScriptJob class allows to submit Python scripts and Jupyter notebooks to the pyiron job management system.

    Args:
        project (ProjectHDFio): ProjectHDFio instance which points to the HDF5 file the job is stored in
        job_name (str): name of the job, which has to be unique within the project

    Simple example:
        Step 1. Create the notebook to be submitted, for ex. 'example.ipynb', and save it -- Can contain any code like:
                ```
                import json
                with open('script_output.json','w') as f:
                    json.dump({'x': [0,1]}, f)  # dump some data into a JSON file
                ```

        Step 2. Create the submitter notebook, for ex. 'submit_example_job.ipynb', which submits 'example.ipynb' to the
                pyiron job management system, which can have the following code:
                ```
                from pyiron_base import Project
                pr = Project('scriptjob_example')  # save the ScriptJob in the 'scriptjob_example' project
                scriptjob = pr.create.job.ScriptJob('scriptjob')  # create a ScriptJob named 'scriptjob'
                scriptjob.script_path = 'example.ipynb'  # specify the PATH to the notebook you want to submit.
                ```

        Step 3. Check the job table to get details about 'scriptjob' by using:
                ```
                pr.job_table()
                ```

        Step 4. If the status of 'scriptjob' is 'finished', load the data from the JSON file into the
                'submit_example_job.ipynb' notebook by using:
                ```
                import json
                with open(scriptjob.working_directory + '/script_output.json') as f:
                    data = json.load(f)  # load the data from the JSON file
                ```

    More sophisticated example:
        The script in ScriptJob can also be more complex, e.g. running its own pyiron calculations.
        Here we show how it is leveraged to run a multi-core atomistic calculation.

        Step 1. 'example.ipynb' can contain pyiron_atomistics code like:
                ```
                from pyiron_atomistics import Project
                pr = Project('example')
                job = pr.create.job.Lammps('job')  # we name the job 'job'
                job.structure = pr.create.structure.ase_bulk('Fe')  # specify structure

                # Optional: get an input value from 'submit_example_job.ipynb', the notebook which submits
                #   'example.ipynb'
                number_of_cores = pr.data.number_of_cores
                job.server.cores = number_of_cores

                job.run()  # run the job

                # save a custom output, that can be used by the notebook 'submit_example_job.ipynb'
                job['user/my_custom_output'] = 16
                ```

        Step 2. 'submit_example_job.ipynb', can then have the following code:
                ```
                from pyiron_base import Project
                pr = Project('scriptjob_example')  # save the ScriptJob in the 'scriptjob_example' project
                scriptjob = pr.create.job.ScriptJob('scriptjob')  # create a ScriptJob named 'scriptjob'
                scriptjob.script_path = 'example.ipynb'  # specify the PATH to the notebook you want to submit.
                                                         # In this example case, 'example.ipynb' is in the same
                                                         # directory as 'submit_example_job.ipynb'

                # Optional: to submit the notebook to a queueing system
                number_of_cores = 1  # number of cores to be used
                scriptjob.server.cores = number_of_cores
                scriptjob.server.queue = 'cmfe'  # specify the queue to which the ScriptJob job is to be submitted
                scriptjob.server.run_time = 120  # specify the runtime limit for the ScriptJob job in seconds

                # Optional: save an input, such that it can be accessed by 'example.ipynb'
                pr.data.number_of_cores = number_of_cores
                pr.data.write()

                # run the ScriptJob job
                scriptjob.run()
                ```

        Step 3. Check the job table by using:
                ```
                pr.job_table()
                ```
                in addition to containing details on 'scriptjob', the job_table also contains the details of the child
                'job/s' (if any) that were submitted within the 'example.ipynb' notebook.

        Step 4. Reload and analyse the child 'job/s': If the status of a child 'job' is 'finished', it can be loaded
                into the 'submit_example_job.ipynb' notebook using:
                ```
                job = pr.load('job')  # remember in Step 1., we wanted to run a job named 'job', which has now
                                      # 'finished'
                ```
                this loads 'job' into the 'submit_example_job.ipynb' notebook, which can be then used for analysis,
                ```
                job.output.energy_pot[-1]  # via the auto-complete
                job['user/my_custom_output']  # the custom output, directly from the hdf5 file
                ```

    Attributes:

        attribute: job_name

            name of the job, which has to be unique within the project

        .. attribute:: status

            execution status of the job, can be one of the following [initialized, appended, created, submitted, running,
                                                                      aborted, collect, suspended, refresh, busy, finished]

        .. attribute:: job_id

            unique id to identify the job in the pyiron database

        .. attribute:: parent_id

            job id of the predecessor job - the job which was executed before the current one in the current job series

        .. attribute:: master_id

            job id of the master job - a meta job which groups a series of jobs, which are executed either in parallel or in
            serial.

        .. attribute:: child_ids

            list of child job ids - only meta jobs have child jobs - jobs which list the meta job as their master

        .. attribute:: project

            Project instance the jobs is located in

        .. attribute:: project_hdf5

            ProjectHDFio instance which points to the HDF5 file the job is stored in

        .. attribute:: job_info_str

            short string to describe the job by it is job_name and job ID - mainly used for logging

        .. attribute:: working_directory

            working directory of the job is executed in - outside the HDF5 file

        .. attribute:: path

            path to the job as a combination of absolute file system path and path within the HDF5 file.

        .. attribute:: version

            Version of the hamiltonian, which is also the version of the executable unless a custom executable is used.

        .. attribute:: executable

            Executable used to run the job - usually the path to an external executable.

        .. attribute:: library_activated

            For job types which offer a Python library pyiron can use the python library instead of an external executable.

        .. attribute:: server

            Server object to handle the execution environment for the job.

        .. attribute:: queue_id

            the ID returned from the queuing system - it is most likely not the same as the job ID.

        .. attribute:: logger

            logger object to monitor the external execution and internal pyiron warnings.

        .. attribute:: restart_file_list

            list of files which are used to restart the calculation from these files.

        .. attribute:: job_type

            Job type object with all the available job types: ['ExampleJob', 'SerialMaster', 'ParallelMaster', 'ScriptJob',
                                                               'ListMaster']

        .. attribute:: script_path

            the absolute path to the python script
    """

    def __init__(self, project, job_name):
        super(ScriptJob, self).__init__(project, job_name)
        self.__version__ = "0.1"
        self.__hdf_version__ = "0.2.0"
        self.__name__ = "Script"
        self._script_path = None
        self.input = DataContainer(table_name="custom_dict")
        self._enable_mpi4py = False

    @property
    def script_path(self):
        """
        Python script path

        Returns:
            str: absolute path to the python script
        """
        return self._script_path

    @script_path.setter
    def script_path(self, path):
        """
        Python script path

        Args:
            path (str): relative or absolute path to the python script or a corresponding notebook
        """
        if isinstance(path, str):
            self._script_path = self._get_abs_path(path)
            self.executable = self._executable_command(
                working_directory=self.working_directory,
                script_path=self._script_path,
                enable_mpi4py=self._enable_mpi4py,
                cores=self.server.cores,
            )
            if self._enable_mpi4py:
                self.executable._mpi = True
        else:
            raise TypeError(
                "path should be a string, but ", path, " is a ", type(path), " instead."
            )

    def enable_mpi4py(self):
        if not self._enable_mpi4py:
            self.executable = self._executable_command(
                working_directory=self.working_directory,
                script_path=self._script_path,
                enable_mpi4py=True,
                cores=self.server.cores,
            )
            self.executable._mpi = True
        self._enable_mpi4py = True

    def disable_mpi4py(self):
        if self._enable_mpi4py:
            self.executable = self._executable_command(
                working_directory=self.working_directory,
                script_path=self._script_path,
                enable_mpi4py=False,
                cores=self.server.cores,
            )
            self.executable._mpi = False
        self._enable_mpi4py = False

    def validate_ready_to_run(self):
        if self.script_path is None:
            raise TypeError(
                "ScriptJob.script_path expects a path but got None. Please provide a path before "
                + "running."
            )

    def set_input_to_read_only(self):
        """
        This function enforces read-only mode for the input classes, but it has to be implement in the individual
        classes.
        """
        self.input.read_only = True

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store the ScriptJob in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(ScriptJob, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            hdf5_input["path"] = self._script_path
            self.input.to_hdf(hdf5_input)

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the ScriptJob from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(ScriptJob, self).from_hdf(hdf=hdf, group_name=group_name)
        if "HDF_VERSION" in self.project_hdf5.list_nodes():
            version = self.project_hdf5["HDF_VERSION"]
        else:
            version = "0.1.0"
        if version == "0.1.0":
            with self.project_hdf5.open("input") as hdf5_input:
                try:
                    self.script_path = hdf5_input["path"]
                    gp = GenericParameters(table_name="custom_dict")
                    gp.from_hdf(hdf5_input)
                    for k in gp.keys():
                        self.input[k] = gp[k]
                except TypeError:
                    pass
        elif version == "0.2.0":
            with self.project_hdf5.open("input") as hdf5_input:
                try:
                    self.script_path = hdf5_input["path"]
                except TypeError:
                    pass
                self.input.from_hdf(hdf5_input)
        else:
            raise ValueError("Cannot handle hdf version: {}".format(version))

    def write_input(self):
        """
        Copy the script to the working directory - only python scripts and jupyter notebooks are supported
        """
        if self._script_path is not None:
            file_name = os.path.basename(self._script_path)
            shutil.copyfile(
                src=self._script_path,
                dst=os.path.join(self.working_directory, file_name),
            )

    def collect_output(self):
        """
        Collect output function updates the master ID entries for all the child jobs created by this script job, if the
        child job is already assigned to an master job nothing happens - master IDs are not overwritten.
        """
        for job in self.project.iter_jobs(recursive=False, convert_to_object=False):
            pr_job = self.project.open(
                os.path.relpath(job.working_directory, self.project.path)
            )
            for subjob_id in pr_job.get_job_ids(recursive=False):
                if pr_job.db.get_item_by_id(subjob_id)["masterid"] is None:
                    pr_job.db.item_update({"masterid": str(job.job_id)}, subjob_id)

    def run_if_lib(self):
        """
        Compatibility function - but library run mode is not available
        """
        raise NotImplementedError(
            "Library run mode is not implemented for script jobs."
        )

    def collect_logfiles(self):
        """
        Compatibility function - but no log files are being collected
        """
        pass

    @staticmethod
    def _executable_command(
        working_directory, script_path, enable_mpi4py=False, cores=1
    ):
        """
        internal function to generate the executable command to either use jupyter or python

        Args:
            working_directory (str): working directory of the current job
            script_path (str): path to the script which should be executed in the working directory
            enable_mpi4py (bool): flag to enable mpi4py
            cores (int): number of cores to use

        Returns:
            str: executable command
        """
        file_name = os.path.basename(script_path)
        path = os.path.join(working_directory, file_name)
        if file_name[-6:] == ".ipynb":
            return (
                "jupyter nbconvert --ExecutePreprocessor.timeout=9999999 --to notebook --execute "
                + path
            )
        elif file_name[-3:] == ".py" and not enable_mpi4py:
            return "python " + path
        elif file_name[-3:] == ".py" and enable_mpi4py:
            return ["mpirun", "-np", str(cores), "python", path]
        else:
            raise ValueError("Filename not recognized: ", path)

    def _executable_activate_mpi(self):
        """
        Internal helper function to switch the executable to MPI mode
        """
        pass

    @staticmethod
    def _get_abs_path(path):
        """
        internal function to convert absolute or relative paths to absolute paths, using os.path.normpath,
        os.path.abspath and os.path.curdir

        Args:
           path (str): relative or absolute path

        Returns:
            str: absolute path
        """
        return os.path.normpath(os.path.join(os.path.abspath(os.path.curdir), path))
