# Examples
Demonstration of a workflow implemented with `pyiron_base`. Based on the history of `pyiron_base` being developed as a
part of `pyiron_atomistics` the example covers the implementation of a workflow for the density functional theory (DFT)
simulation code [quantum espresso](https://www.quantum-espresso.org). As a first step to interface with the quantum 
espresso DFT simulation code the `write_input()` and the `collect_output()` function:
```python
import os
from ase.io import write


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
```
The `write_input()` function takes a dictionary `input_dict` and the path to the working directory `working_directory` 
as inputs and then writes the input files into the working directory. In this example the `write()` function from the
[atomic simulation environment](https://wiki.fysik.dtu.dk/ase/index.html) is used to write the input files. 

Analog to the `write_input()` function the `collect_output()` function gets the `working_directory` as an input and then
parses the files in the working directory to return the output as a dictionary.
```python
from pwtools import io


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
```
For the parsing of the output files of the quantum espresso DFT simulation code the [pwtools](https://elcorto.github.io/pwtools/)
package is used. It can parse both, static calculation as well as structure optimizations or molecular dynamic 
trajectories. 

Finally, the third function is the workflow which combines multiple quantum espresso DFT simulation. In this example 
the workflow initially optimizes the lattice structure followed by the calculation of the change of energy over a series
of five different strains ranging from 90% to 110%. 
```python
import numpy as np


def workflow(project, structure): 
    # Structure optimization 
    job_qe_minimize = project.create.job.QEJob(job_name="qe_relax")
    job_qe_minimize.input["calculation"] = "vc-relax"
    job_qe_minimize.input.structure = structure
    job_qe_minimize.run()
    structure_opt = job_qe_minimize.output.structure

    # Energy Volume Curve 
    energy_lst, volume_lst = [], []
    for i, strain in enumerate(np.linspace(0.9, 1.1, 5)):
        structure_strain = structure_opt.copy()
        structure_strain.set_cell(
            structure_strain.cell * strain**(1/3), 
            scale_atoms=True
        )
        job_strain = project.create.job.QEJob(
            job_name="job_strain_" + str(i)
        )
        job_strain.input.structure = structure_strain
        job_strain.run(delete_existing_job=True)
        energy_lst.append(job_strain.output.energy)
        volume_lst.append(job_strain.output.volume)
    
    return {"volume": volume_lst, "energy": energy_lst}
```
After the definition of the individual functions it is time to put the different parts together. This part again starts
by importing the required modules. For the `pyiron_base` workflow framework the `Project` class is imported. 
```python
from ase.build import bulk
import matplotlib.pyplot as plt
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
After creating the quantum espresso job class using the `create_job_class()` function which takes the `write_input()`
function, the `collect_output()` function, the executable and the default input as input, the actual execution of the 
workflow comes down to three simple steps. First the creation of the job object instance using the `wrap_python_function()`
followed by setting the input parameters, in this case the `project` instance and the atomistic structure created with 
the [atomistic simulation environment](https://wiki.fysik.dtu.dk/ase/index.html) and finally executing the workflow 
using the `run()` function. As a last step the energy volume curve is plotted with the [matplotlib](https://matplotlib.org)
library. 