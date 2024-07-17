from typing import Optional
from pydantic import BaseModel, Field


class ICD10CategoryRequestClass(BaseModel):
    name: str

class ICD10RequestClass(BaseModel):
    code: str
    name: str
    category: Optional[str]
