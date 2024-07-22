# utils.py
from typing import Dict, List
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel


async def populate_user_fields(data: Dict = {}, specific_fields: List[str] = [], db: StandardDatabase = None) -> Dict:
    return BaseResponseModel.populate_user_fields(data, specific_fields, db)
