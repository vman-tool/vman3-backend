from typing import List
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import ICD10, ICD10Category
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

async def create_icd10_codes(codes, user, db: StandardDatabase = None):
    try:
        created_codes = []
        for code in codes:
            category_uuid = code.category
            collection = db.collection(db_collections.ICD10_CATEGORY)
            category_cursor = collection.find({
                'uuid': category_uuid
            })
            category = [category_value for category_value in category_cursor][0]
            code.category = category
            code = code.model_dump()
            code['created_by']=user['uuid']
            saved_code = ICD10(**code).save(db)
            created_codes.append(saved_code)
        return created_codes
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")