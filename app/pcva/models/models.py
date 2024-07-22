from arango.database import StandardDatabase
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
    coder1: Optional[str]
    coder2: Optional[str]

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ASSIGNED_VA


class CodedVA(VmanBaseModel):
    assigned_va: str
    immediate_cod: str
    intermediate_cod: Optional[str]
    intermediate_cod: Optional[str]
    underlying_cod: Optional[str]
    contributory_cod: Optional[str]
    clinical_notes: Optional[str]
    datetime: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.CODED_VA