# Examples
Demonstrate a series of workflows implemented with `pyiron_base`. This should be the same workflow as they are 
demonstrated in the example notebooks. 
## Parameter Study for a Python Function
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

## Parameter Study for an External Executable 
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

## Up-scaling a Simulation Protocol 

### Python Function 
```python
import os
import matplotlib.pyplot as plt
import numpy as np
from ase.build import bulk
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

### Jupyter Notebook 
Explain how to submit a Jupyter notebook with a `ScriptJob`

```python
from pyiron_base import Project
pr = Project("demo")
script = pr.create_job(pr.job_type.ScriptJob, "script")
script.script_path = "demo.ipynb"
script.run()
```
