from arango.database import StandardDatabase
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel
from app.users.models.user import User

from datetime import datetime
from typing import List, Optional
from pydantic import Field


class ICD10Category(VManBaseModel):
    name: str

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ICD10_CATEGORY

class ICD10(VManBaseModel):
    code: str
    name: str
    category: Optional[str] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ICD10

class AssignedVA(VManBaseModel):
    vaId: str
    coder: Optional[str]

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ASSIGNED_VA


class CodedVA(VManBaseModel):
    assigned_va: str
    immediate_cod: str
    intermediate1_cod: Optional[str]
    intermediate2_cod: Optional[str]
    intermediate3_cod: Optional[str]
    underlying_cod: Optional[str]
    contributory_cod: Optional[List[str]]
    clinical_notes: Optional[str]
    datetime: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.CODED_VA