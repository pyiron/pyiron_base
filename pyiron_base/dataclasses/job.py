from dataclasses import dataclass
from typing import Optional, List


@dataclass
class Executable:
    version: Optional[str]
    name: str
    operation_system_nt: bool
    executable: Optional[str]
    mpi: bool
    accepted_return_codes: List[int]
