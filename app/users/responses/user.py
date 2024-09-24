from datetime import datetime
from typing import List, Union

from arango import Optional
from pydantic import BaseModel, EmailStr
from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.configs.models import BaseResponseModel
from app.shared.utils.response import populate_user_fields
from app.users.responses.base import BaseResponse


class UserResponse(BaseResponse):
    uuid: str
    id: int
    name: str
    email: EmailStr
    is_active: bool
    created_at: Union[str, None, datetime] = None
    
    

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    token_type: str = "Bearer"
    user: Optional[UserResponse] = None


class RoleResponse(BaseResponseModel):
    name: str
    privileges: Union[List[str], None] = None

    @classmethod
    async def get_structured_role(cls, role_uuid = None, role = None, db: StandardDatabase = None):
        role = role
        if not role:
            query = f"""
            FOR role IN {db_collections.ROLES}
                FILTER role.uuid == @role_uuid
                RETURN role
            """
            bind_vars = {'role_uuid': role_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            role = cursor.next()
        populated_role_data = await populate_user_fields(role, db=db)
        return cls(**populated_role_data)