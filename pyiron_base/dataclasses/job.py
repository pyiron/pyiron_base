from dataclasses import dataclass
from typing import Optional


@dataclass
class Server:
    user: str
    host: str
    run_mode: str
    cores: int
    gpus: Optional[int]
    threads: int
    new_hdf: bool
    accept_crash: bool
    run_time: Optional[int]  # [seconds]
    memory_limit: Optional[str]
    queue: Optional[str]
    qid: Optional[int]
    additional_arguments: dict
    conda_environment_name: Optional[str]
    conda_environment_path: Optional[str]
