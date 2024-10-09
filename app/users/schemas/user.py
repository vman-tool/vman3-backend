

from typing import Dict, List, Optional, Union

from pydantic import BaseModel, EmailStr


class RegisterUserRequest(BaseModel):
    uuid: Union[str, None] = None
    name: Union[str, None] = None
    email: Union[str, None] = None
    password: Union[str, None] = None
    confirm_password: Union[str, None] = None
    created_by: Union[str, None] = None
    is_active: Union[bool, None] = True
    
    
class VerifyUserRequest(BaseModel):
    token: str
    email: EmailStr
    
class EmailRequest(BaseModel):
    email: EmailStr
    
class ResetRequest(BaseModel):
    token: str
    email: EmailStr
    password: str

class RoleRequest(BaseModel):
    uuid: Union[str, None] = None
    name: str
    privileges: Union[List[str], None] = None

class AssignRolesRequest(BaseModel):
    user: str
    roles: Union[List[str], None] = None
    access_limit: Union[Dict, None] = None
    