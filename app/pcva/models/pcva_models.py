from arango.database import StandardDatabase
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel
from app.users.models.user import User

from datetime import datetime
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field


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
    coder: str

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
    clinicalNotes: Optional[str]
    datetime: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.CODED_VA
    
class FrameA(BaseModel):
    a: Union[str, None] = None
    timeinterval_a: Union[str, None] = None
    b: Union[str, None] = None
    timeinterval_b: Union[str, None] = None
    c: Union[str, None] = None
    timeinterval_c: Union[str, None] = None
    d: Union[str, None] = None
    timeinterval_d: Union[str, None] = None
    contributories: Union[List[str], None] = None

class FrameB(BaseModel):
    surgeryPerformed: Union[str, None] = None
    surgeryDate: Union[str, None] = None
    surgeryReasons: Union[str, None] = None
    autopsyRequested: Union[str, None] = None
    wereFindingsUsedInCertification: Union[str, None] = None

class MannerOfDeath(BaseModel):
    manner: Union[str, None] = None
    dateOfInjury: Union[str, None] = None
    howExternalOrPoisoningAgent: Union[str, None] = None

class PlaceOfOccurence(BaseModel):
    place: Union[str, None] = None
    specific: Union[str, None] = None

class FetalOrInfant(BaseModel):
    multiplePregnancy: Union[str, None] = None
    stillBorn: Union[str, None] = None
    hoursSurvived: Union[int, None] = None
    birthWeight: Union[float, None] = None
    completedWeeksOfPregnancy: Union[int, None] = None
    ageOfMother: Union[int, None] = None
    mothersConditionToNewborn: Union[str, None] = None

class PregnantDeceased(BaseModel):
    pregnancyStatus: Union[str, None] = None
    pregnantTime: Union[str, None] = None
    didPregnancyContributed: Union[str, None] = None

class PCVAResults(VManBaseModel):
    assigned_va: str
    frameA: Union[FrameA, Dict, None] = None
    frameB: Union[FrameB, Dict, None] = None
    mannerOfDeath: Union[MannerOfDeath, Dict, None] = None
    placeOfOccurence: Union[PlaceOfOccurence, Dict, None] = None
    fetalOrInfant: Union[FetalOrInfant, Dict, None] = None
    pregnantDeceased: Union[PregnantDeceased, Dict, None] = None
    clinicalNotes: Union[str, None] = None
    datetime: Union[str, None] = Field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.PCVA_RESULTS