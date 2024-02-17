def get_thread_executor(max_workers):
    from concurrent.futures import ThreadPoolExecutor

    return ThreadPoolExecutor(max_workers=max_workers)


def get_process_executor(max_workers):
    from concurrent.futures import ProcessPoolExecutor

    return ProcessPoolExecutor(max_workers=max_workers)


def get_pympipool_mpi_executor(max_workers):
    from pympipool.mpi.executor import PyMPIExecutor

    return PyMPIExecutor(max_workers=max_workers)


def get_pympipool_slurm_executor(max_workers):
    from pympipool.slurm.executor import PySlurmExecutor

    return PySlurmExecutor(max_workers=max_workers)


def get_pympipool_flux_executor(max_workers):
    from pympipool.flux.executor import PyFluxExecutor

    return PyFluxExecutor(max_workers=max_workers)


EXECUTORDICT = {
    "ThreadPoolExecutor": get_thread_executor
    "ProcessPoolExecutor": get_process_executor,
    "PyMPIPoolExecutor": get_pympipool_mpi_executor,
    "PyMPIPoolSlurmExecutor": get_pympipool_slurm_executor,
    "PyMPIPoolFluxExecutor": get_pympipool_flux_executor,
}