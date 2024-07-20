

from typing import Optional

from pydantic import BaseModel, EmailStr


class RegisterUserRequest(BaseModel):
    name: str
    email: str
    password: str

    def add_created_by(self, created_by):
        self.created_by = created_by
        return self
    
    
class VerifyUserRequest(BaseModel):
    token: str
    email: EmailStr
    
class EmailRequest(BaseModel):
    email: EmailStr
    
class ResetRequest(BaseModel):
    token: str
    email: EmailStr
    password: str
    