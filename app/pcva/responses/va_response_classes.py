from typing import Any, Dict, List, Optional, Union
from fastapi import HTTPException
from pydantic import BaseModel
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel, ResponseUser
from app.shared.utils.response import populate_user_fields
from app.shared.configs.constants import db_collections
from app.users.models.user import User
from app.pcva.models.pcva_models import FetalOrInfant, FrameA, FrameB, MannerOfDeath, PlaceOfOccurence, PregnantDeceased


class ICD10FieldClass(BaseModel):
    uuid: str
    code: str
    name: str

    @classmethod
    async def get_icd10(cls, icd10_uuid, db: StandardDatabase = None):
        try:
            query = f"""
            FOR code IN {db_collections.ICD10}
                FILTER code.uuid == @icd10_uuid
                RETURN {{
                    uuid: code.uuid,
                    code: code.code,
                    name: code.name,
                }}
            """
            bind_vars = {'icd10_uuid': icd10_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            icd10_data = cursor.next()
            return cls(**icd10_data)
        except Exception as e:
            return None

class AssignedVAFieldClass(BaseModel):
    uuid: str
    coder: Optional[ResponseUser]
    vaId: Any = None

    @classmethod
    async def get_structured_assignment_by_vaId(cls, vaId = None, coder = None, db: StandardDatabase = None):
        
        assignment_data = {}
        if vaId and coder:
            query = f"""
            FOR assignment IN {db_collections.ASSIGNED_VA}
                FILTER assignment.vaId == @vaId AND assignment.coder == @coder
                RETURN assignment
            """
            bind_vars = {
                'vaId': vaId,
                'coder': coder
            }
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            assignment_data = cursor.next()
        
        if len(assignment_data.items()) > 0:
            populated_code_data = await populate_user_fields(data = assignment_data, specific_fields = ['coder'], db = db)
            return cls(**populated_code_data)
        return cls()


class AssignVAResponseClass(BaseResponseModel):
    uuid: str
    coder: Optional[ResponseUser]
    vaId: Any = None
    
    @classmethod
    async def get_structured_assignment(cls, assignment_uuid = None, assignment = None, db: StandardDatabase = None):
        assignment_data = assignment
        
        if not assignment_data:
            query = f"""
            FOR assignment IN {db_collections.ASSIGNED_VA}
                FILTER assignment.uuid == @assignment_uuid
                RETURN assignment
            """
            bind_vars = {'assignment_uuid': assignment_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            assignment_data = cursor.next()
        populated_code_data = await populate_user_fields(assignment_data, ['coder'], db)

        
        return cls(**populated_code_data)
    
    @classmethod
    async def populate_va(cls, vaId = None, db: StandardDatabase = None):
        
        va_data = {}
        if vaId:
            query = f"""
            FOR va IN {db_collections.VA_TABLE}
                FILTER va.__id == @vaId
                RETURN va
            """
            bind_vars = {'vaId': vaId}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            va_data = cursor.next()
        
        if len(va_data.items()) > 0:
            return cls(**va_data)
        return cls()
    
    @classmethod
    async def get_structured_assignment_by_vaId(cls, vaId = None, db: StandardDatabase = None):
        
        assignment_data = {}
        if vaId:
            query = f"""
            FOR assignment IN {db_collections.ASSIGNED_VA}
                FILTER assignment.vaId == @vaId
                RETURN assignment
            """
            bind_vars = {'vaId': vaId}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            assignment_data = cursor.next()
        
        if len(assignment_data.items()) > 0:
            populated_code_data = await populate_user_fields(assignment_data, ['coder'], db = db)
            return cls(**populated_code_data)
        return cls()

class CodedVAResponseClass(BaseResponseModel):
    uuid: str
    assignedVA: Optional[AssignedVAFieldClass]
    immediate_cod: Optional[ICD10FieldClass] = None
    intermediate1_cod: Optional[ICD10FieldClass] = None
    intermediate2_cod: Optional[ICD10FieldClass] = None
    underlying_cod: Optional[ICD10FieldClass] = None
    contributory_cod: Optional[List[ICD10FieldClass]] = None

    
    @classmethod
    async def get_structured_codedVA(cls, coded_va = None, db: StandardDatabase = None):
        coded_va_data = coded_va
        if not coded_va_data:
            query = f"""
            FOR coded_va IN {db_collections.CODED_VA}
                FILTER coded_va.coded_va == @coded_va
                RETURN coded_va
            """
            bind_vars = {'coded_va': coded_va}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            coded_va_data = cursor.next()
        
        populated_coded_va_data = await populate_user_fields(coded_va_data, db = db)

        populated_coded_va_data['immediate_cod'] = await ICD10FieldClass.get_icd10(coded_va_data.get("immediate_cod", None), db)
        
        populated_coded_va_data['intermediate1_cod'] = await ICD10FieldClass.get_icd10(coded_va_data.get("intermediate1_cod", None), db)
        
        populated_coded_va_data['intermediate2_cod'] = await ICD10FieldClass.get_icd10(coded_va_data.get("intermediate2_cod", None), db)
        
        populated_coded_va_data['intermediate3_cod'] = await ICD10FieldClass.get_icd10(coded_va_data.get("intermediate2_cod", None), db)
        
        populated_coded_va_data['underlying_cod'] = await ICD10FieldClass.get_icd10(coded_va_data.get("underlying_cod", None), db)
        
        populated_coded_va_data['contributory_cod'] = [await ICD10FieldClass.get_icd10(cod, db) for cod in coded_va_data.get("contributory_cod", []) 
        if cod]
        
        populated_coded_va_data['assignedVA'] = await AssignedVAFieldClass.get_structured_assignment_by_vaId(vaId = coded_va_data['assigned_va'],coder = populated_coded_va_data.get('created_by').uuid, db = db)
        return cls(**populated_coded_va_data)
    
class PCVAResultsResponseClass(BaseModel):
    assigned_va: AssignedVAFieldClass
    frameA: Union[FrameA, Dict, None] = None
    frameB: Union[FrameB, Dict, None] = None
    mannerOfDeath: Union[MannerOfDeath, Dict, None] = None
    placeOfOccurence: Union[PlaceOfOccurence, Dict, None] = None
    fetalOrInfant: Union[FetalOrInfant, Dict, None] = None
    pregnantDeceased: Union[PregnantDeceased, Dict, None] = None
    clinicalNotes: Union[str, None] = None
    datetime: Union[str, None] = None

    @classmethod
    async def get_structured_codedVA(cls, pcva_result_uuid = None, pcva_result = None, db: StandardDatabase = None):
        coded_va_data = pcva_result
        if not coded_va_data:
            query = f"""
            FOR coded_va IN {db_collections.PCVA_RESULTS}
                FILTER coded_va.uuid == @coded_va
                RETURN coded_va
            """
            bind_vars = {'coded_va': pcva_result_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            coded_va_data = cursor.next()

        # Restructure VA document assigned and coded... (Commented as a reserve code)

        coded_va_data['assigned_va'] = await AssignedVAFieldClass.get_structured_assignment_by_vaId(vaId = coded_va_data['assigned_va'], coder = coded_va_data.get('created_by', None), db = db)
        
        populated_coded_va_data = await populate_user_fields(coded_va_data, db = db)

        frameA = coded_va_data.get('frameA', None)

        frameA['a'] = await ICD10FieldClass.get_icd10(frameA.get("a", None), db)
        
        frameA['b'] = await ICD10FieldClass.get_icd10(frameA.get("b", None), db)
        
        frameA['c'] = await ICD10FieldClass.get_icd10(frameA.get("c", None), db)
        
        frameA['d'] = await ICD10FieldClass.get_icd10(frameA.get("d", None), db)
        
        frameA['contributories'] = [await ICD10FieldClass.get_icd10(cod, db) for cod in frameA.get("contributories", None) or [] 
        if cod]

        coded_va_data['frameA'] = frameA
        
        return cls(**populated_coded_va_data)
    

class Option(BaseModel):
    path: str
    value: str
    label: str

class VAQuestionResponseClass(BaseModel):
    path: str
    name: str
    type: str
    binary: Optional[bool] = None
    selectMultiple: Optional[bool] = None
    label: str
    options: Optional[List[Option]] = None


class CoderResponseClass(BaseModel):
    uuid: str
    name: str
    email: Union[str, None] = None
    phone: Union[str, None] = None
    assigned_va: Union[int, None] = None
    coded_va: Union[int, None] = None