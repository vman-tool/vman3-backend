from datetime import datetime
from typing import List, Optional, Union

from arango.database import StandardDatabase
from fastapi import HTTPException
from pydantic import EmailStr

from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel
from app.users.models.role import Role


class User(VManBaseModel):
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
    
    async def save(self, db: StandardDatabase):
        self.init_collection(db)

        collection = db.collection(db_collections.USERS)

        cursor = cursor =collection.find({'email': self.email}, limit=1)
        result = [doc for doc in cursor]
        
        user_exist =  result
        if user_exist:
            raise HTTPException(status_code=400, detail="Email already exists.")

        user = await super().save(db)

        # cursor = collection.find({'email': self.email}, limit=1)
        # user = [doc for doc in cursor]
        
        # check if user exist with new data
        if user['new']:
            user=user['new']
        else:
            user=user
        data={
            "id": user["_key"],
            "uuid": user["uuid"],
            "name": user["name"],
            "email": user["email"],
            "is_active": user["is_active"],
            "created_at":user["created_at"]
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