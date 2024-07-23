from typing import List, Optional
from pydantic import BaseModel


class AssignVARequestClass(BaseModel):
    vaIds: List[str]
    coder1: Optional[str]
    coder2: Optional[str]

class CodeAssignedVARequestClass(BaseModel):
    assigned_va: str
    immediate_cod: Optional[str] = None
    intermediate1_cod: Optional[str] = None
    intermediate2_cod: Optional[str] = None
    underlying_cod: Optional[str] = None
    contributory_cod: Optional[List[str]] = None
    clinical_notes: str