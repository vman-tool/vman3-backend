from typing import Any, Dict, List, Optional
from fastapi import HTTPException
from pydantic import BaseModel
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel, ResponseUser
from app.shared.utils.response import populate_user_fields
from app.shared.configs.constants import db_collections
from app.users.models.user import User


class ICD10FieldClass(BaseModel):
    uuid: str
    code: str
    name: str

    @classmethod
    async def get_icd10(cls, icd10_uuid, db: StandardDatabase = None):
        
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

class AssignedVAFieldClass(BaseModel):
    uuid: str
    coder1: Optional[ResponseUser]
    coder2: Optional[ResponseUser]
    vaId: Any = None

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
            populated_code_data = await populate_user_fields(assignment_data, ['coder1', 'coder2'], db = db)
            return cls(**populated_code_data)
        return cls()


class AssignVAResponseClass(BaseResponseModel):
    uuid: str
    coder1: Optional[ResponseUser]
    coder2: Optional[ResponseUser]
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
        
        populated_code_data = await populate_user_fields(assignment_data, ['coder1', 'coder2'], db)
        
        return cls(**populated_code_data)
    
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
            populated_code_data = await populate_user_fields(assignment_data, ['coder1', 'coder2'], db = db)
            return cls(**populated_code_data)
        return cls()

class CodedVAResponseClass(BaseResponseModel):
    uuid: str
    assignedVA: Optional[AssignedVAFieldClass]
    vaId: Any = None
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

        populated_coded_va_data['immediate_cod'] = await ICD10FieldClass.get_icd10(coded_va_data["immediate_cod"], db) if "immediate_cod" in coded_va_data else ICD10FieldClass()
        
        populated_coded_va_data['intermediate1_cod'] = await ICD10FieldClass.get_icd10(coded_va_data["intermediate1_cod"], db) if "intermediate1_cod" in coded_va_data else ICD10FieldClass()
        
        populated_coded_va_data['intermediate2_cod'] = await ICD10FieldClass.get_icd10(coded_va_data["intermediate2_cod"], db) if "intermediate2_cod" in coded_va_data else ICD10FieldClass()
        
        populated_coded_va_data['underlying_cod'] = await ICD10FieldClass.get_icd10(coded_va_data["underlying_cod"], db) if "underlying_cod" in coded_va_data else ICD10FieldClass()
        
        populated_coded_va_data['contributory_cod'] = [await ICD10FieldClass.get_icd10(cod, db) for cod in coded_va_data["contributory_cod"]] if "contributory_cod" in coded_va_data and coded_va_data["contributory_cod"] else [ICD10FieldClass()]
        
        populated_coded_va_data['assignedVA'] = await AssignedVAFieldClass.get_structured_assignment_by_vaId(coded_va_data['assigned_va'], db)
        return cls(**populated_coded_va_data)