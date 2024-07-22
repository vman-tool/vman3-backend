from typing import List, Optional
from pydantic import BaseModel


class AssignVARequestClass(BaseModel):
    vaIds: List[str]
    coder1: Optional[str]
    coder2: Optional[str]

class CodeAssignedVARequestClass(BaseModel):
    assigned_va: str
    immediate_cod: Optional[str]
    intermediate_cod: Optional[str]
    intermediate_cod: Optional[str]
    underlying_cod: Optional[str]
    contributory_cod: Optional[str]
    clinical_notes: str