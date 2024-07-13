from typing import List
from app.shared.configs.models import VmanBaseModel


class Role(VmanBaseModel):
    name: str
    privileges: List[str]
