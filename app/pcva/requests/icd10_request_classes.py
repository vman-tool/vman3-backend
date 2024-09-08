from typing import Optional, Union
from pydantic import BaseModel, Field


class ICD10CategoryRequestClass(BaseModel):
    name: str

class ICD10CategoryUpdateClass(BaseModel):
    uuid: Union[str, None] = None
    name: str

class ICD10CreateRequestClass(BaseModel):
    code: str
    name: str
    category: Optional[str]
class ICD10UpdateRequestClass(BaseModel):
    uuid: Optional[str]
    code: Union[str, None] = None
    name: Union[str, None] = None
    category: Union[str, None] = None
