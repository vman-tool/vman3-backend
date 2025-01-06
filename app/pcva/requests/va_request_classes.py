from typing import Dict, List, Optional, Union
from pydantic import BaseModel

from app.pcva.models.pcva_models import FetalOrInfant, FrameA, FrameB, MannerOfDeath, PlaceOfOccurrence, PregnantDeceased


class AssignVARequestClass(BaseModel):
    vaIds: List[str]
    coder: Union[str, None] = None
    new_coder: Union[str, None] = None 

class CodeAssignedVARequestClass(BaseModel):
    assigned_va: str
    immediate_cod: Optional[str] = None
    intermediate1_cod: Optional[str] = None
    intermediate2_cod: Optional[str] = None
    intermediate3_cod: Optional[str] = None
    underlying_cod: Optional[str] = None
    contributory_cod: Optional[List[str]] = None
    clinical_notes: str

class PCVAResultsRequestClass(BaseModel):
    assigned_va: str
    frameA: Union[FrameA, Dict, None] = None
    frameB: Union[FrameB, Dict, None] = None
    mannerOfDeath: Union[MannerOfDeath, Dict, None] = None
    placeOfOccurrence: Union[PlaceOfOccurrence, Dict, None] = None
    fetalOrInfant: Union[FetalOrInfant, Dict, None] = None
    pregnantDeceased: Union[PregnantDeceased, Dict, None] = None
    clinical_notes: Union[str, None] = None