from typing import List
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import ICD10Category
from app.pcva.responses.icd10_response_classes import ICD10CategoryResponseClass
from app.shared.configs.constants import db_collections

async def create_icd10_categories(categories, user, db: StandardDatabase = None):
    try:
        created_categories = []
        for category in categories:
            category = category.model_dump()
            category['created_by']=user['uuid']
            saved_category = ICD10Category(**category).save(db)
            created_categories.append(saved_category)
        return created_categories
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")