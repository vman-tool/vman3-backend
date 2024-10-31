from datetime import datetime
from typing import List, Optional, Union

from arango.database import StandardDatabase
from fastapi import HTTPException
from pydantic import EmailStr

from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel
from app.shared.utils.database_utilities import record_exists
from app.users.models.role import Role


class User(VManBaseModel):
    # id: Union[str, None] = None
    name: str
    email: EmailStr
    is_active: bool
    verified_at: Optional[Union[str, datetime]] = None
    tokens: Union[str, None] = None
    roles: Union[List[Role], None] = None
    password: str
    image: Union[str, None] = None
    
   
    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USERS
    
    async def save(self, db: StandardDatabase):
        user_exist =  await record_exists(db_collections.USERS, custom_fields={"email": self.email}, db=db)
        
        if user_exist:
            raise HTTPException(status_code=400, detail="Email already exists.")

        user = await super().save(db)

        data={
            "id": user["_key"] if "_key" in user else None,
            "uuid": user["uuid"] if "uuid" in user else None,
            "name": user["name"] if "name" in user else None,
            "email": user["email"] if "email" in user else None,
            "is_active": user["is_active"] if "is_active" in user else None,
            "created_by":user["created_by"] if "created_by" in user else None,
            "created_at":user["created_at"] if "created_at" in user else None,
            "image": user["image" if "image" in user else None]
        }
        return data
    
    async def update(self, updated_by: str = None, db: StandardDatabase = None):
        
        user = await super().update(updated_by, db)

        data={
            "id": user["_key"] if "_key" in user else None,
            "uuid": user["uuid"] if "uuid" in user else None,
            "name": user["name"] if "name" in user else None,
            "email": user["email"] if "email" in user else None,
            "is_active": user["is_active"] if "is_active" in user else None,
            "created_by":user["created_by"] if "created_by" in user else None,
            "created_at":user["created_at"] if "created_at" in user else None,
            "image": user["image"] if "image" in user else None
        }
        return data




class UserToken(VManBaseModel):
    id: Optional[str]
    user_id: str
    access_key: Optional[str] = None
    refresh_key: Optional[str] = None
    expires_at: Optional[Union[str, datetime]] = None
    user: Optional[User] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USER_TOKENS