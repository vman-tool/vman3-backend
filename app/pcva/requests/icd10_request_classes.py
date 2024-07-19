from typing import Optional
from pydantic import BaseModel, Field


class ICD10CategoryRequestClass(BaseModel):
    name: str

class ICD10CreateRequestClass(BaseModel):
    code: str
    name: str
    category: Optional[str]
class ICD10UpdateRequestClass(BaseModel):
    uuid: str
    code: str
    name: str
    category: Optional[str]
