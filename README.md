# pyiron_base

[![codecov](https://codecov.io/gh/pyiron/pyiron_base/graph/badge.svg?token=qjRvhGVij2)](https://codecov.io/gh/pyiron/pyiron_base)
[![Release_Date](https://anaconda.org/conda-forge/pyiron_base/badges/latest_release_date.svg)](https://anaconda.org/conda-forge/pyiron_base/)
[![Pipeline](https://github.com/pyiron/pyiron_base/actions/workflows/pipeline.yml/badge.svg)](https://github.com/pyiron/pyiron_base/actions/workflows/pipeline.yml)
[![Downloads](https://anaconda.org/conda-forge/pyiron_base/badges/downloads.svg)](https://anaconda.org/conda-forge/pyiron_base/)
[![Documentation Status](https://readthedocs.org/projects/pyiron-base/badge/?version=latest)](https://pyiron-base.readthedocs.io/en/latest/?badge=latest)

The `pyiron_base` workflow manager provides the data storage and job management for the [pyiron](https://pyiron.org)
project. As part of the modularization of the [pyiron](https://pyiron.org) project in 2018, the monolithic code base
which started as `pyCMW` back in 2011 was split in `pyiron_base` and `pyiron_atomistics`. This split highlights the 
separation of the technical complexity and workflow management in `pyiron_base` and the physics modelling for atomistic 
simulation in `pyiron_atomistics`.

## Features:

* Calculation which can be either simple python functions or external executables written in any programming language 
  can be wrapped in `pyiron_base` to enable parameter studies with thousands or millions of calculation.
* The calculation can either be executed locally on the same computer or on high performance computing (HPC) resources.
  The python simple queuing system adapter [pysqa](https://pysqa.readthedocs.io) is used to interface with the HPC 
  queuing systems directly from python and the [pympipool](https://executorlib.readthedocs.io) package is employed to 
  assign dedicated resources like multiple CPU cores and GPUs to individual python functions.
* Scientific data is efficiently stored using the [hierarchical data format (HDF)](https://www.hdfgroup.org) via the 
  [h5py](https://www.h5py.org) python library and more specifically the [h5io](https://github.com/h5io) packages to 
  match the python datatypes to the HDF5 data types.

With this functionality the `pyiron_base` workflow manager enables the rapid prototyping and up-scaling of parameter 
studies for a wide range of scientific application. Starting from simulation codes written in Fortran without any Python
bindings, over more modern modelling codes written in C or C++ with Python bindings up to machine learning models 
requiring GPU acceleration, the approach follows the same three steps:

* Implement a wrapper for the simulation code, which takes a set of input parameters calls the simulation code and
  returns a set of output parameters. For a simulation code with python bindings this is achieved with the 
  `wrap_python_function()` function and for any external executable which requires file-based communication this is 
  achieved with the `create_job_class()` function which requires only a `write_input()` function and a `collect_output()`
  function to parse the input and output files of the external executable. Both functions return a `job` object. This is
  the central building block of the `pyiron_base` workflow manager.
* Following the map-reduce pattern a series of `job` objects are created and submitted to the available computing 
  resources. When the `pyiron_base` workflow manager is executed directly on the login node of a HPC cluster, the
  calculation are directly submitted to the queuing system. Alternatively, the `pyiron_base` workflow manager also 
  supports submission via an secure shell (SSH) connection to the HPC cluster. Still in contrast to many other workflow 
  managers, the `pyiron_base` workflow manager does not require constant connection to the remote computing resources. 
  Once the `job` objects are submitted the workflow can be shutdown.
* Finally, after the execution of the individual `job` objects is completed the `pyiron_table` object gathers the data 
  of the individual `job` objects in a single table. The table is accessible as `pandas.DataFrame` so it is compatible 
  to most machine learning and plotting libraries for further analysis.

## Example:
As the `pyiron_base` workflow manager was developed as part of the [pyiron](https://pyiron.org) project the
implementation of the [quantum espresso](https://www.quantum-espresso.org) density functional theory (DFT) simulation
code in the `pyiron_base` workflow manager is chosen as example. Still the same steps apply for any kind of simulation 
code:
```python
 import os
 import matplotlib.pyplot as plt
 import numpy as np
 from ase.build import bulk
 from ase.calculators.espresso import Espresso
 from ase.io import write
 from pwtools import io


 def write_input(input_dict, working_directory="."):
     filename = os.path.join(working_directory, 'input.pwi')
     os.makedirs(working_directory, exist_ok=True)
     write(
         filename=filename,
         images=input_dict["structure"],
         Crystal=True,
         kpts=input_dict["kpts"],
         input_data={"calculation": input_dict["calculation"]},
         pseudopotentials=input_dict["pseudopotentials"],
         tstress=True,
         tprnfor=True
     )


 def collect_output(working_directory="."):
     filename = os.path.join(working_directory, 'output.pwo')
     try:
         return {"structure": io.read_pw_md(filename)[-1].get_ase_atoms()}
     except TypeError:
         out = io.read_pw_scf(filename)
         return {
             "energy": out.etot,
             "volume": out.volume,
         }


 def workflow(project, structure):
     # Structure optimization
     job_qe_minimize = pr.create.job.QEJob(job_name="qe_relax")
     job_qe_minimize.input["calculation"] = "vc-relax"
     job_qe_minimize.input.structure = structure
     job_qe_minimize.run()
     structure_opt = job_qe_minimize.output.structure

     # Energy Volume Curve
     energy_lst, volume_lst = [], []
     for i, strain in enumerate(np.linspace(0.9, 1.1, 5)):
         structure_strain = structure_opt.copy()
         structure_strain = structure.copy()
         structure_strain.set_cell(
             structure_strain.cell * strain**(1/3),
             scale_atoms=True
         )
         job_strain = pr.create.job.QEJob(
             job_name="job_strain_" + str(i)
         )
         job_strain.input.structure = structure_strain
         job_strain.run(delete_existing_job=True)
         energy_lst.append(job_strain.output.energy)
         volume_lst.append(job_strain.output.volume)

     return {"volume": volume_lst, "energy": energy_lst}


 from pyiron_base import Project
 pr = Project("test")
 pr.create_job_class(
     class_name="QEJob",
     write_input_funct=write_input,
     collect_output_funct=collect_output,
     default_input_dict={  # Default Parameter
         "structure": None,
         "pseudopotentials": {"Al": "Al.pbe-n-kjpaw_psl.1.0.0.UPF"},
         "kpts": (3, 3, 3),
         "calculation": "scf",
     },
     executable_str="mpirun -np 1 pw.x -in input.pwi > output.pwo",
 )

 job_workflow = pr.wrap_python_function(workflow)
 job_workflow.input.project = pr
 job_workflow.input.structure = bulk('Al', a=4.15, cubic=True)
 job_workflow.run()

 plt.plot(job_workflow.output.result["volume"], job_workflow.output.result["energy"])
 plt.xlabel("Volume")
 plt.ylabel("Energy")
```

After the definition of the `write_input()` and `collect_output()` function for the quantum espresso DFT simulation code
the `workflow()` function is defined to combine multiple quantum espresso DFT simulation. First the structure is 
optimized to identify the equilibrium volume and afterwards five strains ranging from 90% to 110% are applied to 
determine the bulk modulus. Finally, in the last few lines all the individual pieces are put together, by creating 
`QEJob` the quantum espresso job class based on the `write_input()` and `collect_output()` function and then wrapping 
the `workflow()` function using the `wrap_python_function()`. The whole workflow is executed when the `run()` function 
is called. Afterwards the results are plotted using the `matplotlib` library.

## Disclaimer
While we try to develop a stable and reliable software library, the development remains a opensource project under the
BSD 3-Clause License without any warranties:
```
BSD 3-Clause License

Copyright (c) 2018, Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## Documentation

* [Installation](https://pyiron-base.readthedocs.io/en/latest/installation.html)
  * [Try pyiron_base online](https://pyiron-base.readthedocs.io/en/latest/installation.html#try-pyiron-base-online)
  * [Workstation Installation](https://pyiron-base.readthedocs.io/en/latest/installation.html#workstation-installation)
  * [HPC Cluster Installation](https://pyiron-base.readthedocs.io/en/latest/installation.html#hpc-cluster-installation)
* [Tutorial](https://pyiron-base.readthedocs.io/en/latest/tutorial.html)
  * [Jupyter Lab](https://pyiron-base.readthedocs.io/en/latest/tutorial.html#jupyter-lab)
  * [Project](https://pyiron-base.readthedocs.io/en/latest/tutorial.html#project)
  * [Job](https://pyiron-base.readthedocs.io/en/latest/tutorial.html#job)
  * [Table](https://pyiron-base.readthedocs.io/en/latest/tutorial.html#table)
* [Example](https://pyiron-base.readthedocs.io/en/latest/example.html)
* [Command Line](https://pyiron-base.readthedocs.io/en/latest/commandline.html)
* [Developers](https://pyiron-base.readthedocs.io/en/latest/developer.html)
  * [Install from Source](https://pyiron-base.readthedocs.io/en/latest/developer.html#install-from-source)
  * [HDF5 Serialization](https://pyiron-base.readthedocs.io/en/latest/developer.html#hdf5-serialization)
  * [Run function](https://pyiron-base.readthedocs.io/en/latest/developer.html#run-function)
  * [Queuing System](https://pyiron-base.readthedocs.io/en/latest/developer.html#queuing-system)
  * [Command Line](https://pyiron-base.readthedocs.io/en/latest/developer.html#command-line)
* [FAQ](https://pyiron-base.readthedocs.io/en/latest/faq.html)
