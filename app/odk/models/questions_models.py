
from typing import List
from arango import Optional
from pydantic import BaseModel
from app.shared.configs.models import VManBaseModel
from app.shared.configs.constants import db_collections


class Option(BaseModel):
    path: str
    value: str
    label: str

class VA_Question(VManBaseModel):
    path: str
    name: str
    type: str
    binary: Optional[bool] = None
    selectMultiple: Optional[bool] = None
    label: str
    options: Optional[List[Option]] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.VA_QUESTIONS