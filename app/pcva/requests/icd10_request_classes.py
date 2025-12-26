from typing import Optional, Union
from pydantic import BaseModel, Field


class ICD10CategoryTypeRequestClass(BaseModel):
    name: str
    description: Optional[str] = None


class ICD10CategoryTypeUpdateClass(BaseModel):
    uuid: str
    name: Union[str, None] = None
    description: Optional[str] = None


class ICD10CategoryRequestClass(BaseModel):
    name: str
    type: str
    description: Optional[str] = None


class ICD10CategoryUpdateClass(BaseModel):
    uuid: str
    name: Union[str, None] = None
    type: Union[str, None] = None
    description: Union[str, None] = None

class ICD10CreateRequestClass(BaseModel):
    code: str
    name: str
    category: Optional[str]
class ICD10UpdateRequestClass(BaseModel):
    uuid: Optional[str]
    code: Union[str, None] = None
    name: Union[str, None] = None
    category: Union[str, None] = None
