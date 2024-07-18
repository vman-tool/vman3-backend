from typing import List, Optional
from pydantic import BaseModel


class AssignVARequestClass(BaseModel):
    vaIds: List[str]
    coder1: Optional[str]
    coder2: Optional[str]