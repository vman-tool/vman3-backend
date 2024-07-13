from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, EmailStr

from app.shared.configs.arangodb_db import get_arangodb_client
from app.users.models.role import Role


class User(BaseModel):
    id: str
    name: str
    email: EmailStr
    is_active: bool
    created_at: Optional[Union[str, datetime]] = None
    updated_at: Optional[Union[str, datetime]] = None
    verified_at: Optional[Union[str, datetime]] = None
    tokens: Optional[str] = None
    created_by: Optional[str] = None
    roles: List[Role] = []
    
    async def save(self, colletion_name:str):
        print('st ---')
        # creator = await get_current_user()
        # print(creator,'test ---')
        # self.created_by  = creator['_key']
        client = await get_arangodb_client()
        collection = client.db.collection(colletion_name)
        collection.insert(self.model_dump())



class UserToken(BaseModel):
    id: str
    user_id: str
    access_key: Optional[str] = None
    refresh_key: Optional[str] = None
    created_at: Optional[Union[str, datetime]] = None
    expires_at: Optional[Union[str, datetime]] = None
    user: Optional[User] = None