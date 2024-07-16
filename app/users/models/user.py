from datetime import datetime
from typing import List, Optional, Union

from arango.database import StandardDatabase
from fastapi import HTTPException
from pydantic import BaseModel, EmailStr

from app.shared.configs.constants import db_collections
from app.shared.configs.models import VmanBaseModel
from app.shared.configs.security import hash_password, is_password_strong_enough
from app.users.models.role import Role


class User(VmanBaseModel):
    # id: Optional[str] = None
    name: str
    email: EmailStr
    is_active: bool
    verified_at: Optional[Union[str, datetime]] = None
    tokens: Optional[str] = None
    roles: Optional[List[Role]] = None
    password: str
    
   
    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USERS
    
    def save(self, db: StandardDatabase):
        super().init_collection(db)

        collection = db.collection(db_collections.USERS)

        cursor = cursor =collection.find({'email': self.email}, limit=1)
        result = [doc for doc in cursor]
        
        user_exist =  result
        if user_exist:
            raise HTTPException(status_code=400, detail="Email already exists.")

        super().save(db)

        cursor = collection.find({'email': self.email}, limit=1)
        user = [doc for doc in cursor]

        return {
            "id": user[0]["_key"],
            "name": user[0]["name"],
            "email": user[0]["email"],
            "is_active": user[0]["is_active"],
            "created_at":user[0]["created_at"]
        }




class UserToken(VmanBaseModel):
    id: Optional[str]
    user_id: str
    access_key: Optional[str] = None
    refresh_key: Optional[str] = None
    created_at: Optional[Union[str, datetime]] = None
    expires_at: Optional[Union[str, datetime]] = None
    user: Optional[User] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USER_TOKENS