from typing import List
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.models import ICD10, ICD10Category
from app.pcva.responses.icd10_response_classes import ICD10CategoryResponseClass, ICD10ResponseClass
from app.shared.configs.constants import db_collections
from app.users.models.user import User

async def create_icd10_categories_service(categories, user, db: StandardDatabase = None):
    try:
        created_categories = []
        for category in categories:
            category = category.model_dump()
            category['created_by']=user['uuid']
            saved_category = ICD10Category(**category).save(db)
            created_category = ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
            created_categories.append(created_category)
        return created_categories
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")

async def get_icd10_codes(paging: bool = True,  page_number: int = 1, page_size: int = 10, include_deleted: bool = None, db: StandardDatabase = None) -> List[ICD10ResponseClass]:
    try:
        data = [
            await ICD10ResponseClass.get_structured_code(icd10_code = icd10_code, db = db) 
            for icd10_code in await ICD10.get_many(
                paging = paging, 
                page_number = page_number, 
                page_size = page_size, 
                include_deleted = include_deleted,
                db = db
            )]
        return {
            "page_number": page_number,
            "page_size": page_size,
            "data": data
        } if paging else data
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to get codes: {e}")

async def create_icd10_codes(codes, user, db: StandardDatabase = None) -> List[ICD10ResponseClass]:
    try:
        created_codes = []
        for code in codes:
            code = code.model_dump()
            code['created_by']=user['uuid']
            code.pop("uuid") if 'uuid' in code else code
            saved_code = ICD10(**code).save(db)
            created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
            created_codes.append(created_code)
        return created_codes
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")

async def update_icd10_codes(codes, user, db: StandardDatabase = None) -> List[ICD10ResponseClass]:
    try:
        created_codes = []
        for code in codes:
            code_json = code.model_dump()
            saved_code = ICD10(**code_json).update(user['uuid'], db)
            created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
            created_codes.append(created_code)
        return created_codes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update categories: {e}")