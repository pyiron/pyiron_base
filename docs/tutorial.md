# Tutorial 
The motivation for the `pyiron_base` workflow manager is to provide a self-consistent environment for the development
of simulation protocols. Many `pyiron_base` users previously used the command line to manage their simulation protocols
and construct parameter studies. Still based on the limitations in coupling command line approaches to machine learning 
models, `pyiron_base` is based on the Python programming language, and it is recommended to use the `pyiro_base` workflow
manager in combination with Jupyter Lab for maximum efficiency. Consequently, the first challenge is guiding users who 
used the command line before and now switch to using only Jupyter notebooks. 

The three most fundamental objects to learn when interacting with the `pyiron_base` workflow manager are jupyter 
notebooks, `pyiron_base` Project objects and `pyiron_base` Job objects. These are introduced below. 

## Jupyter Lab
By default, Jupyter notebooks consist of three types of cells: `<code>` cells, `<raw>` cells and `<markdown>` cells. To
execute python code, select the `<code>` cells. These cells can then be executed using `<enter> + <shift>`. While it is
technically possible to execute cells in Jupyter notebooks in arbitrary order, for reproducibility it is recommended to
execute the individual cells in order. 

To learn more about Jupyter notebooks, checkout the [Jupyter notebook documentation](https://jupyter-notebook.readthedocs.io/).

To start a jupyter lab in a given directory call the `jupyter lab` command: 
```commandline
jupyter lab
```
This opens a jupyter lab session in your web browser, typically on port `8888`.

Alternatively, the `pyiron_base` workflow manager can also be executed in a regular python shell or python script, still
for most users the jupyter lab environment offers more flexibility and simplifies the development and documentation of
simulation workflows.

## Project 
To encourage rapid prototyping the `pyiron_base` workflow manager requires just a single import statement. So to start
using `pyiron_base` import the `Project` class: 
```python
from pyiron_base import Project
```
The `Project` class represents a folder on the file system. Instead of hiding all calculation in an abstract database
the `pyiron_base` workflow manager leverages the file system to store results of calculation. To initialize a `Project`
object instance just provide a folder name. This folder is then created right next to the Jupyter notebook. 
```python
pr = Project(path="demonstration")
```
The project object instance is the central object of the `pyiron_base` workflow manager. All other object instances are
created from the project object instance. To get the full path of the project object instance is using the `path` 
property:
```python
pr.path
```
To accelerate the development of simulation protocols the `pyiron_base` workflow manager benefits from the `<tab>` based
autocompletion. If you type `pr.p` and press `<tab>` the jupyter notebook environment automatically completes your entry.

In addition to the representation of the project object instance as a folder on the file system, the project object 
instance is also connected to the SQL database. Using the `job_table()` function the job objects in a given project are
listed. By default, the `pyiron_base` workflow manager uses the SQL database for quickly generating a list of all job
objects. Still the `pyiron_base` workflow manager can also be installed without an SQL database. In that case the `job_table()`
function generates a list of job objects by iterating over the file system. 
```python
pr.job_table()
```
When the project was just started, the job table is expected to be an empty `pandas.DataFrame`. To get an overview of 
all the parameters of the `job_table()` function the jupyter lab environment provides the question mark parameter to 
look up the documentation: 
```python
pr.job_table?
```
Furthermore, with two question marks it is also possible to take a look at the source code of a given python function:
```python
pr.job_table??
```

In the same way the `job_table()` function can be used to get a list of all job objects in a Project object instance the
`remove_jobs()` function can be used to delete all job objects in a project. 
```python
pr.remove_jobs()
```
In many cases it is also useful to just remove a single job object using the `remove_job()` function. This is introduced
below once the job objects are introduced in the section below.

## Job 
The job object class is the second building block of the `pyiron_base` workflow manager. In your parameter study each 
unique combination of a set of parameters is represented as a single job object. In the most typical cases this can be
either the call of a python function or the call of an external executable, still also the aggregation of results in a 
pyiron table is represented as job object. The advantage of representing all these different tasks as job objects is 
that the job objects can be submitted to the queuing system to distribute the individual tasks over the computing 
resources of an HPC cluster. 

### Python Function 
For python functions which run several minutes or hours, it is essential to have a workflow manager. The `pyiron_base`
workflow manager addresses this challenge by storing the input and output of each python function call in an HDF5 file,
which has the advantage that each function call can also be submitted to the queuing system of an HPC cluster. In case
your function only requires a run time of several seconds or minutes it is recommended to combine multiple function calls
in a larger function and then submit this function as a pyiron job to the HPC cluster. This can be achieved by using the
[concurrent.futures.ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor)
inside the function to distribute a number of function calls over the multiple CPU cores of the compute nodes. So the
[concurrent.futures.ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor)
is used for the task distribution inside the compute node and the `pyiron_base` workflow manager is used for the task 
distribution over multiple compute nodes and handling the submission to the queuing system. 

In this example a very simple python function is used: 
```python
def test_function(a, b=8):
    return a+b
```
The run time for this function is far below a millisecond so is it not reasonable to submit it to a remote computing cluster.
This is primarily a demonstration to highlight the capabilities of the `pyiron_base` workflow manager. 
```python
from pyiron_base import Project

pr = Project("test")
job = pr.wrap_python_function(test_function)
job.input["a"] = 4
job.input["b"] = 5
job.run()

job.output["result"] 
>>> 9
```
After the import of the Project class a Project instance is created connected to the folder `test`. Then the `test_function()`
is wrapped as a job object. This allows the user to set the input using the `job.input` property of the job object. The
individual input parameters can be accessed using the edge bracket notation, just like a python dictionary. Finally,
when the `job.run()` function is called the job is executed. After the successful submission the output can be accessed
via the `job.output` property. 

To submit the function call to a remote HPC cluster the server object of the job object can be configured: 
```python
job.server.queue = "slurm"
job.server.cores = 120
job.server.run_time = 3600  # in seconds 
```
In this example the `slurm` queue was selected, a total of 120 CPU cores were selected and a run time of one hour was
selected. Still it is important to mention, that assigning 120 CPU cores does not enable parallel execution of the python
function. Only by implementing internal parallelization inside the python functions with solutions like 
[concurrent.futures.ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor)
it is possible to parallelize the execution of python functions on a single compute node. Finally, the pyiron developers
released the [pympipool](https://pympipool.readthedocs.io) to enable parallelization of python functions as well as the
direct assignment of GPU resources inside a given queuing system allocation over multiple compute nodes using the 
hierarchical queuing system [flux](https://flux-framework.org). 

### External Executable 
As many scientific simulation codes do not have Python bindings the `pyiron_base` workflow manager also supports the 
submission of external executables. In the `pyiron_base` workflow manager external executables are interfaces using three
components, a `write_input()` function, a `collect_output()` function and finally an executable string which is executed
after the input files were written. The `write_input()` function takes a `input_dict` dictionary and a `working_directory`
as input parameters and writes the input files into the working directory. 
```python
import os 

def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))
```
In analogy the `collect_output()` function takes the `working_directory` as an input parameter and returns a dictionary
of the output. 
```python
def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}
```
Once the `write_input()` and `collect_output()` function are defined the actual workflow is defined. Starting with the
definition of the Project object instance, followed by creating the job class using the `create_job_class()` function. 

In this example the `cat` command is used to copy the energy value from the input file to the output file. Again this is
not a function which is typically submitted to a HPC cluster, it is primarily a demonstration how to implement how to 
create a job class based on an external executable plus a `write_input()` and `collect_output()` function. 
```python
from pyiron_base import Project

pr = Project(path="test")
pr.create_job_class(
    class_name="CatJob",
    write_input_funct=write_input,
    collect_output_funct=collect_output,
    default_input_dict={"energy": 1.0},
    executable_str="cat input_file > output_file",
)
job = pr.create.job.CatJob(job_name="job_test")
job.input["energy"] = 2.0
job.run()

job.output["result"]
>>> 2.0
```
In analogy to the job objects for python functions also the python functions for external executables can be submitted 
to the queuing system by configuring the same `job.server` property. Still it is important to update the executable 
string `executable_str` variable to use `mpiexec` or other means of parallelization to execute the external executable
in parallel. 

### Jupyter Notebook
The third category of job objects the `pyiron_base` workflow manager supports are the `ScriptJob` which are used to 
submit Jupyter notebooks to the queuing system. While it is recommended to use the `wrap_python_function()` to wrap the
python commands some users prefer to submit a whole Jupyter notebook at once. So the `pyiron_base` workflow manager 
introduces the `ScriptJob` job type: 
```python
from pyiron_base import Project
pr = Project(path="demo")
script = pr.create.job.ScriptJob(job_name="script")
script.script_path = "demo.ipynb"
script.input['my_variable'] = 5
script.run()
```
The `ScriptJob` jobs take a jupyter notebook as an input `script.script_path` as well as a series of input parameters
in the `script.input`. These input parameters can then be accessed in the jupyter notebook using the `get_external_input()`
function. So an example `demo.ipynb` jupyter notebook could include the following code:
```python
from pyiron_base import Project
pr = Project(path="script")
external_input = pr.get_external_input()
external_input
>>> {"my_variable": 5}
```

## Table 
The third category of central objects in the `pyiron_base` workflow manager next to the project object and the job objects
is the pyiron table object. While technically the pyiron table object is another job object, it is application is primarily
to gather job objects in a given project. The pyiron table object follows the [map-reduce](https://en.wikipedia.org/wiki/MapReduce).

The pyiron table object takes three kinds of inputs. The first is a filter function which is used to identify which jobs
the following functions are applied on. The filter function is not mandatory still it is very helpful in particular when
a large number of jobs are created in a given project. 
```python
def myfilter(job): 
    return "test_function" in job.job_name
```
The second part are the selection of functions which are applied on all job objects in a given project. Again it takes
a job object as an input. Each job is going to be represented as a row in the `pandas.DataFrame` created by the pyiron
job table and each function represents a column. 
```python
def len_job_name(job):
    return len(job.job_name)
```
The third part is a project the pyiron table is applied on. This is required to store the pyiron table in a different 
project compared to the project the job objects are located in. By default the analysis is applied on the project the
pyiron table object is created in. 
```python
from pyiron_base import Project

pr = Project(path="demo")
table = pr.create_table()
table.filter_function = myfilter
table.add["len"] = len_job_name
# table.analysis_project = pr
table.run()

table.get_dataframe()
```
After the definition of optional filter function and the functions as well as the analysis project the table is executed
just like a job object. This means that the pyiron table can also be submitted to the queuing system for large map-reduce
tasks. Finally, the pyiron table collects the results as a `pandas.DataFrame` so the results can be directly used in 
machine learning models or data analysis. 