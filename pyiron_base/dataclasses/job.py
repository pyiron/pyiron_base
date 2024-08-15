from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Executable:
    name: str
    operation_system_nt: bool
    mpi: bool
    accepted_return_codes: List[int]
    version: Optional[str] = None
    executable: Optional[str] = None
