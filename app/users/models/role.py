from typing import List
from app.shared.configs.constants import db_collections
from app.shared.configs.models import VmanBaseModel


class Role(VmanBaseModel):
    name: str
    privileges: List[str]

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.USER_TOKENS
