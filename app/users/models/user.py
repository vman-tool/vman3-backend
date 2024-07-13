from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, EmailStr

from app.shared.configs.models import VmanBaseModel
from app.users.models.role import Role


class User(VmanBaseModel):
    id: Optional[str] = None
    name: str
    email: EmailStr
    is_active: bool
    verified_at: Optional[Union[str, datetime]] = None
    tokens: Optional[List[str]] = None
    roles: Optional [List[Role]] = None
    
    @classmethod
    def get_collection_name(cls) -> str:
        return "users"

    



class UserToken(BaseModel):
    id: str
    user_id: str
    access_key: Optional[str] = None
    refresh_key: Optional[str] = None
    created_at: Optional[Union[str, datetime]] = None
    expires_at: Optional[Union[str, datetime]] = None
    user: Optional[User] = None