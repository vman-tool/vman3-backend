from typing import List, Union
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VManBaseModel


class Role(VManBaseModel):
    name: str
    privileges: Union[List[str], None] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.ROLES

class UserRole(VManBaseModel):
    user: str
    role: str

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USER_ROLES
    
