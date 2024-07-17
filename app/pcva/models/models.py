from app.shared.configs.constants import db_collections
from app.shared.configs.models import VmanBaseModel
from app.users.models.user import User

from datetime import datetime
from typing import Optional
from pydantic import Field


class ICD10Category(VmanBaseModel):
    name: str

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ICD10_CATEGORY

class ICD10(VmanBaseModel):
    code: str
    name: str
    category: Optional[str] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ICD10


class AssignedVA(VmanBaseModel):
    vaId: str
    coder1: Optional[User]
    coder2: Optional[User]

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ASSIGNED_VA


class CodedVA(VmanBaseModel):
    assignedVA: AssignedVA
    coda: ICD10
    codb: Optional[ICD10]
    codc: Optional[ICD10]
    codd: Optional[ICD10]
    codContributory: Optional[ICD10]
    comment: Optional[str]
    datetime: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.CODED_VA