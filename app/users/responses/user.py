from datetime import datetime
from typing import Union

from pydantic import BaseModel, EmailStr

from app.users.responses.base import BaseResponse


class UserResponse(BaseResponse):
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