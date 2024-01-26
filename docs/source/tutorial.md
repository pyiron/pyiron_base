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
Explain how to wrap a python function with `pyiron_base`:

```python
def test_function(a, b=8):
    return a+b

from pyiron_base import Project
pr = Project("test")
job = pr.wrap_python_function(test_function)
job.input["a"] = 4
job.input["b"] = 5
job.run()
job.output["result"] 
```

### External Executable 
```python
import os 

def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}


from pyiron_base import Project
pr = Project("test")
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
job.output
```

### Jupyter Notebook
Explain how to submit a Jupyter notebook with a `ScriptJob`
```python
from pyiron_base import Project
pr = Project("demo")
script = pr.create_job(pr.job_type.ScriptJob, "script")
script.script_path = "demo.ipynb"
script.run()
```

## Table 
Introduce the pyiron table class to aggregate data following the map-reduce pattern. 