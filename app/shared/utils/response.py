# utils.py
from typing import Dict
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel


def populate_user_fields(data: Dict, db: StandardDatabase) -> Dict:
    return BaseResponseModel.populate_user_fields(db, data)
