from typing import Any, Dict, Optional
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
    def get_icd10(cls, icd10_uuid, db: StandardDatabase = None):
        
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
            code_data = cursor.next()
        populated_code_data = await populate_user_fields(code_data, ['coder1', 'coder2'], db)
        
        return cls(**populated_code_data)