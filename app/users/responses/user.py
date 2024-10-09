from datetime import datetime
from typing import Any, Dict, List, Union

from arango import Optional
from pydantic import BaseModel, EmailStr
from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.configs.models import BaseResponseModel, ResponseUser
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
        populated_role_data = await populate_user_fields(data = role, db=db)
        return cls(**populated_role_data)

class UserRolesResponse(BaseResponseModel):
    user: ResponseUser
    roles: Union[List[Dict], None] = None
    access_limit: Union[Dict, None] = None

    @classmethod
    async def get_structured_user_role(cls, user_role_uuid = None, user_role = None, db: StandardDatabase = None):
        user_role = user_role
        if not user_role:
            query = f"""
                LET access_info = (
                    FOR a IN {db_collections.USER_ACCESS_LIMIT}
                        FILTER a.user == @user_uuid AND a.is_deleted == false
                        RETURN a.access_limit
                    )[0]

                FOR user_role IN {db_collections.USER_ROLES}
                    FILTER user_role.user == @user_uuid AND user_role.is_deleted == false

                    // Fetch the role associated with the user_role
                    LET role = (
                        FOR r IN {db_collections.ROLES}
                        FILTER r.uuid == user_role.role
                        RETURN {{ uuid: r.uuid, name: r.name, privileges: r.privileges }}
                    )[0]

                    COLLECT user = user_role.user INTO roleGroups
                    
                    RETURN MERGE(
                        FIRST(roleGroups[*].user_role),
                        {{roles: roleGroups[*].role}},
                        {{access_limit: access_info}}
                    )
            """
            bind_vars = {'user_role_uuid': user_role_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            user_role = cursor.next()
        user_role.pop("role", None)
        populated_role_data = await populate_user_fields(data = user_role, specific_fields=['user'], db=db)
        return cls(**populated_role_data)