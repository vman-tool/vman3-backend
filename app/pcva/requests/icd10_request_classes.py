from typing import Optional
from pydantic import BaseModel, Field


class ICD10CategoryRequestClass(BaseModel):
    name: str

class ICD10CategoryUpdateClass(BaseModel):
    uuid: str
    name: str

class ICD10CreateRequestClass(BaseModel):
    code: str
    name: str
    category: Optional[str]
class ICD10UpdateRequestClass(BaseModel):
    uuid: Optional[str]
    code: Optional[str]
    name: Optional[str]
    category: Optional[str]
