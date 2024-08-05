from pyiron_base.project.generic import Project


def wrap_exe(
    executable_str,
    job_name=None,
    write_input_funct=None,
    collect_output_funct=None,
    input_dict=None,
    conda_environment_path=None,
    conda_environment_name=None,
    input_file_lst=None,
    automatically_rename=False,
    execute_job=False,
    delayed=False,
    output_file_lst=[],
    output_key_lst=[],
):
    """
    Wrap any executable into a pyiron job object using the ExecutableContainerJob.

    Args:
        executable_str (str): call to an external executable
        job_name (str): name of the new job object
        write_input_funct (callable): The write input function write_input(input_dict, working_directory)
        collect_output_funct (callable): The collect output function collect_output(working_directory)
        input_dict (dict): Default input for the newly created job class
        conda_environment_path (str): path of the conda environment
        conda_environment_name (str): name of the conda environment
        input_file_lst (list): list of files to be copied to the working directory before executing it\
        execute_job (boolean): automatically call run() on the job object - default false
        automatically_rename (bool): Whether to automatically rename the job at
            save-time to append a string based on the input values. (Default is
            False.)
        delayed (bool): delayed execution
        output_file_lst (list):
        output_key_lst (list):

    Example:

    >>> def write_input(input_dict, working_directory="."):
    >>>     with open(os.path.join(working_directory, "input_file"), "w") as f:
    >>>         f.write(str(input_dict["energy"]))
    >>>
    >>>
    >>> def collect_output(working_directory="."):
    >>>     with open(os.path.join(working_directory, "output_file"), "r") as f:
    >>>         return {"energy": float(f.readline())}
    >>>
    >>>
    >>> import pyiron_base.mini as pyiron_mini
    >>> job = pyiron_mini.wrap_exe(
    >>>     write_input_funct=write_input,
    >>>     collect_output_funct=collect_output,
    >>>     input_dict={"energy": 1.0},
    >>>     executable_str="cat input_file > output_file",
    >>>     execute_job=True,
    >>> )
    >>> print(job.output)

    Returns:
        pyiron_base.jobs.flex.ExecutableContainerJob: pyiron job object
    """
    return Project(path=".").wrap_executable(
        executable_str=executable_str,
        job_name=job_name,
        write_input_funct=write_input_funct,
        collect_output_funct=collect_output_funct,
        input_dict=input_dict,
        conda_environment_path=conda_environment_path,
        conda_environment_name=conda_environment_name,
        input_file_lst=input_file_lst,
        automatically_rename=automatically_rename,
        execute_job=execute_job,
        delayed=delayed,
        output_file_lst=output_file_lst,
        output_key_lst=output_key_lst,
    )


def wrap_py(
    python_function,
    *args,
    job_name=None,
    automatically_rename=True,
    execute_job=False,
    delayed=False,
    output_file_lst=[],
    output_key_lst=[],
    **kwargs,
):
    """
    Create a pyiron job object from any python function

    Args:
        python_function (callable): python function to create a job object from
        *args: Arguments for the user-defined python function
        job_name (str | None): The name for the created job. (Default is None, use
            the name of the function.)
        automatically_rename (bool): Whether to automatically rename the job at
            save-time to append a string based on the input values. (Default is
            True.)
        delayed (bool): delayed execution
        execute_job (boolean): automatically call run() on the job object - default false
        **kwargs: Keyword-arguments for the user-defined python function

    Returns:
        pyiron_base.jobs.flex.pythonfunctioncontainer.PythonFunctionContainerJob: pyiron job object

    Example:

    >>> def test_function(a, b=8):
    >>>     return a+b
    >>>
    >>> import pyiron_base.mini as pyiron_mini
    >>> job = pyiron_mini.wrap_py(test_function)
    >>> job.input["a"] = 4
    >>> job.input["b"] = 5
    >>> job.run()
    >>> job.output
    >>>
    >>> pyiron_mini.wrap_py(test_function, 4, b=6)

    """
    return Project(path=".").wrap_python_function(
        python_function=python_function,
        *args,
        job_name=job_name,
        automatically_rename=automatically_rename,
        execute_job=execute_job,
        delayed=delayed,
        output_file_lst=output_file_lst,
        output_key_lst=output_key_lst,
        **kwargs,
    )


def cache_list():
    """
    By default pyiron_mini caches the results of completed calculation. With the cache_list() function the content of
    the cache in the current folder including sub-folders is listed.

    Returns:
        pandas.DataFrame: pyiron_mini cache in the current folder including sub-folders
    """
    return Project(path=".").job_table(recursive=True)


def cache_clear(silently: bool = False):
    """
    By default pyiron_mini caches the results of completed calculation. With the cache_clear() function the content of
    the cache in the current folder including sub-folders is cleared.

    Args:
        silently (bool): Enable silent clearing without asking the user for confirmation
    """
    Project(path=".").remove_jobs(recursive=True, silently=silently)


def conda_create_env(
    env_name: str,
    env_file: str,
    use_mamba: bool = False,
    global_installation: bool = True,
):
    """
    Create conda environment to execute selected pyiron tasks in a separate conda environment

    Args:
        env_name (str): Name of the new conda environment
        env_file (str): Path to the conda environment file (environment.yml) which includes the dependencies for the new
                        conda environment.
        use_mamba (bool): Use mamba rather than conda - false by default
        global_installation (bool): Create a global conda environment rather than creating the conda environment in the
                                    current folder - true by default
    """
    Project(path=".").conda_environment.create(
        env_name=env_name,
        env_file=env_file,
        global_installation=global_installation,
        use_mamba=use_mamba,
    )
