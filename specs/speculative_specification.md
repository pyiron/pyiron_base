The purpose of this document is to lay out the high-level view of pyiron's purposes, capabilities, and structure.
Some what's written here represents our ambitions for 2023 and does not necessarily represent the current state of the code base; these ambitional aspects should be clearly indicated as such.

# Why pyiron -- philosophy for prospective users

pyiron exists to facilitate and accelerate computational research.

In particular, pyiron...
- makes easy things easy, and novel things possible
- provides reliability and reproducibility by coupling results to the input and code(TODO) that generated them 
- gives users tools to rapidly prototype new ideas, and easily scale those ideas up
- abstracts away all (or at least most) of the nitty-gritty to let scientists focus on the science


# What pyiron -- promises for actual users

Pyiron provides a python framework for designing and managing research calculations.
It is structured into a network of repositories: `pyiron_base`, which provides the core pyiron infrastructure, and then a variety of `pyiron_X` modules which build all the domain-specific tools for particular fields of research.
E.g. `pyiron_atomistics` support for atomistic-scale materials science calculations, allowing users to create atomistic structures and perform a variety of calculations (minimization, molecular dynamics (MD), and more sophisticated routines such as nudged elastic band, variance-constrained semi-grand-canonical MD, etc.) using a variety of different engines to calculate energies, forces, and perhaps electronic state (e.g. LAMMPS, VASP, SPHiNX, QuantumEspresso...).
Most of these extensions currently focus on materials science, but this is purely historical and `pyiron_base` is designed to support.

Pyiron has a single point of entry, a single import, the `Project` object, from which all other pyiron objects can be created.
Entire projects can be packaged and shared with colleagues.
`Project` can either be imported from `pyiron_base` or from one of the other domain specific modules.
When importing additional `pyiron_X` modules, the `Project` object is _automatically powered-up_ and given access to all the tools from that module.
By heavy use of tab-completion, users can explore what tools are available from their project without having to immediately resort to reading documentation. 

The core of pyiron, and the most common object created from a project, is a `Job`.
Jobs are the objects which actually perform a calculation by invoking their `run` command.
Job objects bring together the core elements of pyiron: they provide an easy to use and easy to extend python interface; they support reproducibility by serializing the details of input and executed code(TODO) with output results; they facilitate analysis and collaboration by populating a database with their most important details; they are scalable by simply specifying which remote resource and how many cores to use.
Importantly, they do all these things without heavy intervention from the user.
Users simply create a job from a project, specify appropriate `input`, (optionally) specify `server` details for HPC calculations, and call the `run()` method.
Under the hood, pyiron then handles all the serialization, registration with the database, and HPC queue and job status management.

The most common type of job is one that wraps some third-party software to performa a calculation.
In this case pyiron provides a simple framework for research management, ensuring that input and results are coupled, and making results easier to share, parse, and analyze.
Pyiron supports Jupyter notebook environments, so users can easily assemble workflows (including visual analysis) in an easy-to-use pure-python environment. 
However, pyiron also provides more sophisticated job types and has templates for various workflow abstractions.
Thus, power-users can go beyond notebook-as-workflow to develop robust and shareable tools for their communities.

Finally, pyiron is also highly customizable.
Configuration for various queueing systems, as well as access to and resources for various third party codes are all possible in the pyiron resources directory.
Other customization, e.g. whether to run with or without a database, is handled by the global `State` object.
Pyiron also supports a variety of working environments, including local (e.g. your laptop), HPC clusters, and even accessing a variety of remote systems, doing calculations there, and collecting results back to another central system.


# How pyiron -- guidance for developers

Database interaction (TODO. Is totally deeply embedded in the pyiron library. It's even mangled so atomistics content lives in base. I am not sure how we want to abstract this. We also don't even serialize what code was used (pyiron versions, called executable versions(this maybe we do store?) to produce the output, much less store it nicely in the database.)

For storage, pyiron uses HDF5 and relies on the pyiron/datacontainer library(TODO) which in turn uses the h5io/h5io library.
With data container objects, users don't need to worry about any of the storage implementation, and most developers only need to worry about assigning the values they wish to store as attributes of a `DataContainer`.
To the extent that users need to know about storage details at all, they should be aware that the can `load` data to completely re-instantiate a python object (potentially slow), or simply `inspect` the data to look at just one part of the data (usually very fast).

Scalability is handled by the pyiron/psqa library, which abstracts the interface with a variety of queueing programs frequently used in high performance computing (HPC).
Scaling calculations from a simple test case on your laptop or HPC head-node is as simple as specifying a number of cores for your job, which HPC `queue` it should run on, and running you job (or group of jobs using python for-loops).
Pyiron also provides support for a variety of different running styles to suit your HPC cluster.
Commercial cloud applications, e.g. AWS, are currently not supported.

Job templates (TODO. We need to continue to refactor the job hierarchy. In particular, all IO needs to be rebased onto `DataContainer`; "interactive" vs "normal" jobs needs clarity (or just everything should be interactive!); templates (parallel, serial, flexible, worker, etc) need to be well distinguished and documented; hierarchy lines need to be better separated, e.g. `python_only_job` and `collect_output` should not both live in `GenericJob`, rather there should be child branches for pure-python and those that wrap an executable; the `run` cycle needs to be simplified and re-worked so that most developers don't even need to worry about it, i.e. no remembering to call `self.status = 'collect'; self.run()` in `self.run_static` or whatever. In the final version of this document we simply need to deliniate how we want it to look at the end of the day, not iterate every problem standing between us and that vision, but I'm not clear what that vision is and I think intermediate problems need to be nailed down in order to clarify that final vision.)

Powering up `Project`. (TODO. We should probably include some technical promises/implementation explanation for how we get access to new tools when we import new `pyiron_X` modules. In particular, we probably need to come to some resolution about domain name conflicts and `pr.create.job.Lammps` (how to handle conflicts?) vs `pr.atomistics.job.Lammps` (is less nice english) vs `pr.create.atomistics.job.Lammps` (is verbose))


# The Zen of pyiron

(TODO. I can't find this! It should be reproduced here)