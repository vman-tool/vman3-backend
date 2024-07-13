

from typing import Optional

from pydantic import BaseModel, EmailStr


class RegisterUserRequest(BaseModel):
    name: str
    email: str
    password: str
    created_by: Optional[str] = None
    
    
class VerifyUserRequest(BaseModel):
    token: str
    email: EmailStr
    
class EmailRequest(BaseModel):
    email: EmailStr
    
class ResetRequest(BaseModel):
    token: str
    email: EmailStr
    password: str
    