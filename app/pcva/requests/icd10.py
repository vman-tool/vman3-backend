from pydantic import BaseModel, Field


class ICD10CategoryRequestClass(BaseModel):
    name: str