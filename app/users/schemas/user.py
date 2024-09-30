

from typing import List, Optional, Union

from pydantic import BaseModel, EmailStr


class RegisterUserRequest(BaseModel):
    name: str
    email: str
    password: str
    created_by: Union[str, None] = None
    
    
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
    